# cloud_computing

**Should not change the folder name "project" **

After cloning the repo.
1. cd to project folder
2. docker-compose up --build
3. Then hit the below APIs in the given order every time .yml file is restarted.
4. The error is When the new container is created by **client.containers.run** function in **app.py** file of the orchestrator folder. The created container is not getting connected to the network **project_default**

API-1: http://0.0.0.0:80/api/v1/db/read  Method: POST
Body: {
    "method": "COUNT"
}

API-2: http://0.0.0.0:80/api/v1/db/write Method: POST
Body: 

{
    "method": "UPDATE",
    "license_plate": "TN41MF6137",
    "entry_time": "00:00:00"
}

API 3: http://0.0.0.0:80/api/v1/crash/slave Method: POST

NOTE: compose-up should be run in a folder named "project" for the network attribute in the client.containers.run to work.
