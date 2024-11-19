import requests

url = "https://sc21jrn-sql-test.azurewebsites.net/api/HttpTrigger?"
body = {"name" : "Python"}

http_post = requests.post(url, json=body)

print (http_post.text)