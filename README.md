# tower-analytics-test-data

![Python application](https://github.com/jameswnl/tower-analytics-test-data/workflows/Python%20application/badge.svg)

API to generate test data for Automation Analytics

## Local setup

### Install
```
  git clone git@github.com:jameswnl/tower-analytics-test-data.git
  cd tower-analytics-test-data.git
  pipenv install
```

### Set Env variables
```
  # path to folder to store created bundles
  BUNDLE_DIR  # Default: /BUNDLE_DIR'
  
  # host url this service is exposed. The service in K8s pod
  # This will be set in the kafka message which processor will find out where to download the bundles from
  HOST_URL
```

###  Authentication
To guard the API service using GitHub OAuth app

(The download bundle endpoint is not blocked by authentication)

* Create a GitHub OAuth app: https://github.com/settings/applications/new
* Set the Authorization callback URL to `http://localhost:8000/-/auth-callback`

```
   GH_AUTH_CLIENT_ID  # Github OAuth App client ID
   GH_AUTH_CLIENT_SECRET  # Github OAuth App client Secret
   ALLOW_GH_ORGS  # Allowed Github organizations. Default: Ansible
```

### Fire it up
```
  pipenv shell
  uvicorn api.main:app
```

### Check it out

open http://localhost:8000/docs


## Docker image

* Build the docker image using the `Dockerfile`
* Set the env for the deployment config


