import streamlit as st
import base64
import os
import json
from snowflake.snowpark.context import get_active_session
from urllib.parse import urlparse

# SnowBees, No-Code API Catalog
# Author: Matteo Consoli 
# Artifact: snow_bees.py 
# Version: v.1.0 
# Date: 13-11-2023

### --------------------------- ###
### Header & Config             ###  
### --------------------------- ###

# Set page title, icon, description
st.set_page_config(
    page_title="SnowBees - Api Catalog",
    page_icon="snow.png",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("❄️ SnowBees 🐝")
st.text("In Italian, the term \"API\" translates to \"bees\" So, welcome to SnowBees, your buzzing hub for API management!")

### ---------------------------- ###
### Sidebar - Configurations     ### 
### ---------------------------- ###

#Logo
image_name = 'logo_snowbees.png'
mime_type = image_name.split('.')[-1:][0].lower() 
if os.path.isfile(image_name):
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.sidebar.image(image_string)
else:
    st.sidebar.write("SnowBees Logo not uploaded in Streamlit App Stage")

#Menu
menu_option = st.sidebar.selectbox("API Catalog Features",["Create New Bee", "List All Bees"])

#Credits
st.sidebar.text("Author: Matteo Consoli")

### --------------------------- ###
### Snowflake Connection        ###  
### --------------------------- ###

def get_snowflake_connection():
        return get_active_session()
st.session_state.snowflake_connection = get_active_session()

### --------------------------- ###
### Create New Bee - Page1      ###  
### --------------------------- ###
st.markdown("""----""")
if menu_option == "Create New Bee":
    st.subheader("Define API Properties")
    #add all default values here and field configs
    apiSecretName='None'
    apiSecretValue='None'
    apiHeader=""
    apiBody=""
    apiParamValue=""
    apiHasParamURL='No'
    
    cols=st.columns(3)
    with cols[0]:
        apiTypeValue = st.selectbox("API Type?", ["GET","POST"],key=f"apiType")
    with cols[1]:
        apiHasSecret = st.selectbox("API Secret?", ['No','Yes'],key=f"apiSecret")
    with cols[2]:
        if apiTypeValue == 'GET':
            apiHasParamURL = st.selectbox("API URL Parametrized? (Max 1 param)", ['No','Yes'])
        
### ------------------------------- ###
### Create New Bee - API Configs    ###  
### ------------------------------- ###
    if apiHasParamURL=="Yes":
        apiURLPlaceholder="https://www.myapi.com/{param}/getInfo"
    else: 
        apiURLPlaceholder="https://www.myapi.com/getInfo"
    apiURL = st.text_input("API URL",placeholder=apiURLPlaceholder,key=f"apiURL")
    if apiHasSecret == 'Yes':
        cols=st.columns(2)
        with cols[0]:
            apiSecretName = st.text_input("API Secret Name",placeholder="Default: secret_<function_name>", key=f"apiSecretName", disabled=True)
        with cols[1]:
            apiSecretValue = st.text_input("API Secret Bearer Token",placeholder="1234-5678-abcd", key=f"apiSecretValue")

    st.markdown("""----""")
 
    st.subheader("Define Snowflake Function")
    snowflakeFunction = st.text_input("Snowflake Function Name (default prefix: 'API_')",placeholder="get_id",key=f"snowFunction")

### ---------------------------- ###
### Create New Bee - Functions   ### 
### ---------------------------- ###
# Functions Definition to dynamically create Network rules, secret, integration and UDF 
    
    # Create the network rule
    def createNetworkRuleQuery (apiName,apiDomain):
        return f"create or replace network rule {apiName} MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('{apiDomain}')";
    
    # Create the token
    def createSecretQuery (secretName, secretString):
        return f"create or replace secret {secretName} TYPE = GENERIC_STRING SECRET_STRING = '{secretString}'";
    
    # Create the integration object
    def createIntegrationObjectQuery (integrationName, networkRuleName, secretName):
        return f"""create or replace external access integration {integrationName}
        ALLOWED_NETWORK_RULES = ({networkRuleName})
        ALLOWED_AUTHENTICATION_SECRETS = ({secretName if secretName!=''else ''})
        ENABLED = TRUE;""";
    
    # Create the function calling the external API - not particularly proud of code readability here
    def apiFunctionQuery (functionName, integrationName, secretName, apiURL, apiType, hasParam):
        if apiType == 'GET' and hasParam == 'No':
            composedFunctionName= functionName+'()'
            composedHandlerName= functionName+'():'
        elif apiType == 'GET' and hasParam != 'No':
            composedFunctionName= functionName+'(parameter string)'
            composedHandlerName= functionName+'(parameter):'
        else: # apiType == 'POST':
            composedFunctionName= functionName+'(headerParam string, bodyParam string)'
            composedHandlerName= functionName+'(headerParam, bodyParam):'
        return f"""CREATE OR REPLACE FUNCTION """+ (composedFunctionName)+f"""
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = 3.10
        HANDLER = '{functionName}'
        EXTERNAL_ACCESS_INTEGRATIONS = ({integrationName})"""+  ('\n\t\tSECRETS= (\'secret_variable\'= '+secretName+')' if secretName != 'None' else '') + f"""
        PACKAGES = ('snowflake-snowpark-python','requests' )
        AS
    $$
import _snowflake
import requests
import json
def """ +composedHandlerName+f"""  
    try: parameter
    except NameError: parameter = ''
    headerFull={{'content-type': 'application/json'}}
    bodyFull={{}}
    if \'{apiType}\' == 'POST':
        # Prepare Header- Split the headerParam string into individual headers
        headers_to_add = headerParam.split('\\n')     
        # Iterate through the headers and add them to headerFull
        for header in headers_to_add:
            if ':' in header:
                name, value = header.split(':', 1)
                headerFull[name.strip()] = value.strip()
        # Prepare Body - Split the input string into lines
        bodyLines = bodyParam.split('\\n')
        # Iterate through the lines and split each line into key and value
        for bodyLine in bodyLines:
            if ':' in bodyLine:
                key, value = bodyLine.split(':')
                bodyFull[key.strip()] = value.strip()
    if \'{secretName}\' != 'None':
        bearer_token=_snowflake.get_generic_secret_string('secret_variable')
        # Additional headers as a string
        additional_headers = {{
            'Authorization': 'Bearer ' + bearer_token
        }}
        headerFull.update(additional_headers)
    apiURL = '{apiURL}'.format(param=parameter)
    response = requests."""+('post' if apiType == 'POST' else 'get')+"""(apiURL , headers = headerFull """ +  (', data = bodyFull' if apiType == 'POST' else '')+f""")
    if response.status_code == 200:
        return response.json()
    else:
        return apiURL + 'Error Code:' + str(response.status_code) + ' Message: ' + response.text
    $$""";

### ------------------------------- ###
### Create New Bee - Engine & Test  ###
### ------------------------------- ###
    
    normalisedFunctionName= f"API_{snowflakeFunction}"
    # Create Function and handle creation of secret only if required.
    if st.button("Create API Function"):
        if snowflakeFunction=='' or apiURL == '':
            st.warning('Missing Function Name or URL')
        else: 
            try: 
                session = get_snowflake_connection()
                apiDomain = urlparse(apiURL).netloc
                networkRuleQueryString = createNetworkRuleQuery(f"network_{snowflakeFunction}",''.join(apiDomain))
                session.sql(f"{networkRuleQueryString}").collect()
                if apiSecretValue !='None':
                    secretQueryString = createSecretQuery(f"secret_{snowflakeFunction}",apiSecretValue)
                    session.sql(f"{secretQueryString}").collect()
                    integrationObjectQueryString = createIntegrationObjectQuery(f"integration_{snowflakeFunction}",f"network_{snowflakeFunction}",f"secret_{snowflakeFunction}")
                    apiFunctionQueryString = apiFunctionQuery(functionName=f"{normalisedFunctionName}",integrationName=f"integration_{snowflakeFunction}",secretName=f"secret_{snowflakeFunction}",apiURL=f"{apiURL}",apiType=apiTypeValue, hasParam=apiHasParamURL)
                else:
                    integrationObjectQueryString = createIntegrationObjectQuery(f"integration_{snowflakeFunction}",f"network_{snowflakeFunction}","")
                    apiFunctionQueryString = apiFunctionQuery(functionName=f"{normalisedFunctionName}",integrationName=f"integration_{snowflakeFunction}",secretName='None',apiURL=f"{apiURL}",apiType=apiTypeValue, hasParam=apiHasParamURL)
                session.sql(integrationObjectQueryString).collect()
                if session.sql(f"{apiFunctionQueryString}").collect():
                    st.success("Function Created Successfully")
            except:
                st.error ('An error occurred creating one of the objects. You can read the error message from the Query History in Snowsight.')
    # Test Function passing testing parameters if required
    testing=False;
    testFunctionQuery=''
    if apiTypeValue == 'POST':
        st.write ("****Testing Parameters****")
        cols=st.columns(2)
        with cols[0]:
            apiHeader = st.text_area("POST Header",placeholder="key: value (one per line): \n\'headerId\': 1 \n\'env\': \'Test\'", key=f"apiHeaderValue")
        with cols[1]:
            apiBody = st.text_area("POST Body", placeholder="key: value (one per line): \n\'id\': 1 \n\'name\': \'Matteo\'",key=f"apiBodyValue")
    if apiHasParamURL == 'Yes':
        st.write ("****Testing Parameters****")
        apiParamValue = st.text_input("Set parameter",placeholder="Replacing the placeholder {param} in the API URL above",key=f"snowFunctionParam")

    if st.button("Test API Function"):
        try:
            session = get_snowflake_connection()
            testing=True
            if apiTypeValue == 'GET' and apiHasParamURL == 'No':
                testFunctionQuery=(f'SELECT {normalisedFunctionName}() as TEST;')
            elif apiTypeValue == 'GET' and apiHasParamURL != 'No':
                testFunctionQuery=(f'SELECT {normalisedFunctionName}(\'{apiParamValue}\') as TEST;')
            else:
                headerNormalised= apiHeader.replace("'", "\\'")
                bodyNormalised= apiBody.replace("'", "\\'")
                testFunctionQuery=(f'SELECT {normalisedFunctionName}(\'{headerNormalised}\',\'{bodyNormalised}\') as TEST;')
            # Show Testing Results
            if testing==True:
                session = get_snowflake_connection()
                st.write('Executing SQL:')
                st.text(testFunctionQuery)
                st.write("API Response:")
                if apiTypeValue == 'GET' and apiParamValue == "":
                    df2= (session.sql(f"SELECT {normalisedFunctionName}() as TEST;").collect()); 
                elif apiTypeValue == 'GET' and apiParamValue != "":
                    df2= (session.sql(f"SELECT {normalisedFunctionName}('{apiParamValue}') as TEST;").collect()); 
                else:
                    headerNormalised= apiHeader.replace("'", "\\'")
                    bodyNormalised= apiBody.replace("'", "\\'")
                    df2= (session.sql(f"SELECT {normalisedFunctionName}('{headerNormalised}','{bodyNormalised}') as TEST;").collect()); 
                df_list = [json.loads(row.asDict()['TEST']) for row in df2]
                st.write(df_list)
        except:
            st.error('An error occurred testing the function '+normalisedFunctionName+'. You can read the error message from the Query History in Snowsight.')

### --------------------------- ###
### List all Bees - Page2       ###  
### --------------------------- ###

# Listing APIs
if menu_option == "List All Bees":
    st.subheader("APIs Catalog")
    apiSignature=''
    dropDependencies=''
    session = get_snowflake_connection()
    st.write("List all the API UDF functions created via SnowBees!")
    df2 = session.sql(f"SELECT FUNCTION_NAME, ARGUMENT_SIGNATURE, TO_DATE(CREATED) as CREATION_DATE FROM INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_NAME LIKE 'API_%'").collect()
    st.table(df2);
# Manage & Drop APIs and dependencies
    st.markdown("""----""")
    st.subheader("Drop API")
    cols=st.columns(2)
    with cols[0]:
        apiSignature = st.text_input("API Name and Signature:",placeholder="api_test(number, number)",key=f"snowFunctionDelete")
    with cols[1]:
        dropDependencies = st.selectbox("Clean Dependencies (if exist)?", ["Yes","No"],key=f"dropDependencies")
    if st.button ("Drop (if exists)"):
        try:
            if apiSignature!= '':
                if session.sql(f"DROP FUNCTION IF EXISTS {apiSignature};").collect():
                    st.success(f"Function '{apiSignature}' dropped Successfully")
                if dropDependencies == 'Yes':
                    if session.sql(f"DROP NETWORK RULE IF EXISTS network_{apiSignature[4:]};").collect():
                        st.success(f"Network Rule 'network_{apiSignature[4:]}' dropped Successfully")
                    if session.sql(f"DROP SECRET IF EXISTS secret_{apiSignature[4:]};").collect():
                        st.success(f"Secret 'secret_{apiSignature[4:]}' dropped Successfully")
                    if session.sql(f"DROP EXTERNAL ACCESS INTEGRATION IF EXISTS integration_{apiSignature[4:]};").collect():
                        st.success(f"External Integration 'integration_{apiSignature[4:]}' dropped Successfully")
            else:
                st.warning('Please, write the API Name and Signature you want to delete.')
        except:
            st.error('An error occurred dropping the function '+apiSignature+'. You can read the error message from the Query History in Snowsight.')

            

### --------------------------- ###
### End Pages                   ###  
### --------------------------- ###



    
