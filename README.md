# Mandatory action

## Configure GCP Project and enable the APIs

**1: Install Gcloud SDK (optional if installed)?**

Install `gcloud SDK`. Please follow this [link](https://cloud.google.com/sdk/docs/install).

After installation, update gcloud to the latest version. 
```gcloud components update```


**2: Gcloud SDK configuration ?**

Configure the gcloud SDK to match project working on
```
gcloud config set project <PROJECT_ID>
gcloud config set compute/region us-east5
```


**3: Enable required APIs?**

```
gcloud services enable cloudresourcemanager.googleapis.com 
gcloud services enable logging.googleapis.com

gcloud services enable dataflow.googleapis.com
gcloud services enable compute.googleapis.com

gcloud services enable artifactregistry.googleapis.com 
gcloud services enable cloudbuild.googleapis.com

gcloud services enable secretmanager.googleapis.com
gcloud services enable sqladmin.googleapis.com
```

**4: Set up environment variables**

```
export PROJECT_ID=<PROJECT_ID>
export PROJECT_NUMBER=<PROJECT_NUMBER>
export REGION=us-east5
```

## Prepare the permissions

**1: Grant permissions to Dataflow runner?**
Grant roles to your Compute Engine default service account (xxxxxxxxxxx-compute@developer.gserviceaccount.com). Run the following command once for each of the following IAM roles:
- roles/dataflow.admin
- roles/dataflow.worker
- roles/storage.objectAdmin
- roles/artifactregistry.reader
- roles/secretmanager.secretAccessor
- roles/bigquery.dataViewer
- roles/bigquery.jobUser
- roles/cloudsql.client
- roles/logging.logWriter
- roles/iam.serviceAccountUser

```
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/dataflow.admin

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/dataflow.worker

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/storage.objectAdmin

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/artifactregistry.reader

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/secretmanager.secretAccessor

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/bigquery.dataViewer

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/bigquery.jobUser

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/cloudsql.client

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/logging.logWriter

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/iam.serviceAccountUser
```


**2: Grant permissions to Airflow instance?**
[Create](https://cloud.google.com/iam/docs/service-accounts-create) and grant service account (the service account should be like this <SERVICE_ACCOUNT_ID>@<PROJECT_ID>.iam.gserviceaccount.com). Run the following command once for each of the following IAM roles:
- roles/dataflow.developer
- roles/dataflow.worker
- roles/iam.serviceAccountUser

```
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/dataflow.developer

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/dataflow.worker

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" --role=roles/iam.serviceAccountUser
```

[Create service account key to authenticate for dataflow](https://cloud.google.com/iam/docs/keys-create-delete).

Upload the downloaded credentials to Airflow Admin Configurations with [default credentials named google_cloud_default](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html#default-connection-ids).




# Deployment action

## Create Dataflow Flex Template

Dataflow Flex Templates allow you to package a Dataflow pipeline for deployment. This tutorial shows you how to build a Dataflow Flex Template and then run a Dataflow job using that template. Please follow this [link](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#local-shell). Below is some main step to create the Dataflow Flex Template.

Prepare environment variables
```
export BUCKET_TEMPLATE=bq-to-cloud-sql-template
export ARTIFACT_REPOSITORY=dataflow-container
export REGION=us-east5
```


Create a Cloud Storage bucket
```
gsutil mb gs://${BUCKET_TEMPLATE}
```

Create an Artifact Registry repository
```
gcloud artifacts repositories create $ARTIFACT_REPOSITORY \
 --repository-format=docker \
 --location=$REGION
```

Authenticate to Artifact repository
```
gcloud auth configure-docker ${REGION}-docker.pkg.dev
```

Build flex template, remember the following srcipts is in `src/migration/app/config/metadata` on this source and remember to modify the file `build-template.sh` with proper project. The file `demo-pipeline-metadata.json` is used to define the metadata of current Dataflow Template like pipeline name or passing parameters.
```
# This is an example of the scripts to build flex template
gcloud dataflow flex-template build \
    "gs://${BUCKET_TEMPLATE}/optimize-pipeline-template.json" \
    --image "us-east5-docker.pkg.dev/${PROJECT_ID}/dataflow-container/pipeline-template-optimize-container:latest" \
    --sdk-language "PYTHON" \
    --metadata-file "demo-pipeline-metadata.json"
```

Build the Dataflow image using the Cloud Build Yaml definitions in `src/migration/pipeline-template.cloudbuild.yaml`. We can use the script is on `src/migration/build-pipeline-template.sh` but remember to modify the information in `pipeline-template.cloudbuild.yaml` to match your project and repository used above in flex template
```
gcloud builds submit --config=pipeline-template.cloudbuild.yaml
```