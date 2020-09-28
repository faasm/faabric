import requests
from invoke import task


@task
def call(ctx, user, func, data=None, host="localhost"):
    msg = {
        "user": user,
        "function": func,
    }

    if data:
        msg["input"] = data

    url = "http://{}:8080/f/{}/{}".format(host, user, func)
    response = requests.post(url, json=msg)

    if response.status_code >= 400:
        print("Request failed: status = {}".format(response.status_code))
        exit(1)

    print(response.text)
