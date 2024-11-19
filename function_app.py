import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import uuid

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name(name="HttpTrigger")
@app.route(route="HttpTrigger", auth_level=func.AuthLevel.ANONYMOUS)
@app.generic_output_binding(
    arg_name="nameUpload",
    type="sql",
    CommandText="dbo.test",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
def HttpTrigger(req: func.HttpRequest, nameUpload : func.Out[func.SqlRow]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.get_json().get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        nameUpload.set(func.SqlRow({
            "id" : str(uuid.uuid4()),
            "name" : name
        }))
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "Please pass a najme on the query string or in the request body",
             status_code=400
        )