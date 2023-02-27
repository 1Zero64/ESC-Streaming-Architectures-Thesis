# Setup Manual
For using the project with the quarkus application and all its technologies some requirements must be met.
The following manual covers the setup with installation and preparation of the technologies.

## Kafka and PostgreSQL Database
In this project is a docker-compose YAML file available with the necessary containers. For that you have to install docker [Docker Desktop](https://docs.docker.com/desktop/) and docker-compose (this step is only necessary for Linux: [Docker Compose](https://docs.docker.com/compose/install/#install-compose)).

After the installation run the Docker Desktop app. Check if docker is running:
```shell script
docker info
```

A lot of information should be displayed if it is running.

### Install containers
To install and run the database and Kafka run the following command:
```shell script
docker-compose up -d
```

A process will be started, where docker pulls the images and installs the wished software. Depending on the network bandwidth this may take some minutes.
When everything was installed and the containers are running the console will display the following messages:

![img_1.png](setup-manual-pictures/img_1.png)

To manage the containers you can also use the Docker Desktop App through a GUI (see the picture below). For manual instructions via command line read the upcoming sections.

![img_7.png](setup-manual-pictures/img_7.png)

### Create Database Schema and Tables
There is no need to create the database schema and tables. When the container is running, the Quarkus application will automatically create all needed tables on start, if no tables exist.

### Create Kafka Topic
When starting the application a topic will be automatically created, but only with 1 partition. To create the data stream with 3 partitions run the following command:
```shell script
docker-compose exec kafka kafka-topics --create --bootstrap-server \
localhost:9092 --replication-factor 1 --partitions 3 --topic measurement-kafka-stream 
```

A Success message will appear in the console.

> **_NOTE:_**  After every remove with docker-compose the topic must be created again. This is not necessary for stop commands.

### Start containers
To start the database and Kafka container run the same command as before:
```shell script
docker-compose up -d
```
Or and after the containers are stopped
```shell script 
docker-compose start
```

These messages will be displayed:

![img_6.png](setup-manual-pictures/img_6.png)

Running the command again will display the following messages:

![img_5.png](setup-manual-pictures/img_5.png)

### Stop containers
To stop the database and Kafka container run the following command:
```shell script
docker-compose stop
```

These messages will appear after stopping the containers:

![img_3.png](setup-manual-pictures/img_3.png)

### Remove containers
To stop and remove the container with all the containers run the following command:
```shell script
docker-compose down
```

These messages will appear after stopping the containers:

![img_4.png](setup-manual-pictures/img_4.png)

## Kinesis Data Streams
Setting up and running Kinesis Data Streams locally is not easy. For that reason this project uses a Kinesis Data Stream hosted on AWS.

> **_NOTE:_**  Kinesis Data Streams has no free tier and will cost something. The costs will be around 1$ for one Data Stream with three shards running for 6 hours and 1 GB of data produced and 1 GB of data consumed.
> You can skip this step and comment out the @Scheduled annotations in the DataCProducer and DataConsumer classes in com.hfu.kauz.event_stream.kinesis package, if you don't want to have costs. The comparison with the other technologies can be found in the thesis, if you are interested.
> If you still want to try out this streaming technology do not forget to delete the data stream after the experiments or the costs will rise up pretty fast!

### Login or create an AWS account
Login in to your AWS account as the root user (not very best practice) under [AWS Sign in](https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fus-east-1.console.aws.amazon.com%2Fconsole%2Fhome%3FhashArgs%3D%2523%26isauthcode%3Dtrue%26nc2%3Dh_ct%26region%3Dus-east-1%26skipRegion%3Dtrue%26src%3Dheader-signin%26state%3DhashArgsFromTB_us-east-1_70704ee32dd40837&client_id=arn%3Aaws%3Asignin%3A%3A%3Aconsole%2Fcanvas&forceMobileApp=0&code_challenge=VFE48hXxvE82dJwZ7m-f9dW3AIO-zjKsCOIsURY8sPw&code_challenge_method=SHA-256).

![img_56.png](setup-manual-pictures/img_56.png)

If you don't have an account create one under [Sign up](https://portal.aws.amazon.com/billing/signup#/start/email).
> **_NOTE:_** A credit card is needed for the registration. After that you will have a [free tier](https://aws.amazon.com/free/?nc1=h_ls&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all) for a lot of different services - like virtual machines or databases - to test them for a year or forever.

After the login you will be forwarded to the AWS management console.

### AWS User Setup
To use AWS services it is recommended to create an AWS user with minimal needed permissions after the Least Privilege principal.

For that search for the IAM service in the searchbar in the AWS management console and select it.

![img_48.png](setup-manual-pictures/img_48.png)

Navigate to users and press the "Add users" button.

![img_49.png](setup-manual-pictures/img_49.png)

AWS will guide you through the creating process. First give the user a name and go to permissions.

![img_50.png](setup-manual-pictures/img_50.png)

In the permissions steps search and select "AmazonKinesisFullAccess".

![img_51.png](setup-manual-pictures/img_51.png)

Skip tags and go to Review. After reviewing create the user.

![img_52.png](setup-manual-pictures/img_52.png)

The access key and secret access key will appear after the creation. Copy the keys and save them (for example as a .csv through the button).
> **_NOTE:_** This will be the last time the credentials will be available for download. However, you can create new credentials at any time.

![img_53.png](setup-manual-pictures/img_53.png)

### Install AWS CLI
AWS Command Line Interface (AWS CLI) is a unified tool to manage AWS services. Download the AWS CLI under https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html.

To verify the installation of AWS CLI and if shell or bash can find and run _aws_ use the following command:
```shell script
aws --version
```

The response could look like this:

![img_54.png](setup-manual-pictures/img_54.png)

### Configure the AWS CLI
After a user was created and the AWS CLI installed it is necessary to configure it, so AWS services can be run through your AWS account.

For that type in the terminal:
```shell script
aws configure
```

The terminal will prompt you step-by-step to configure the AWS user and account.
- Paste in your access key from before (in the AWS User Setup section)
- Paste in your secret access key from before
- Type in eu-central-1 as your region, so the services will be created in this region
- Skip the Default output format by pressing enter

```bash
AWS Access Key ID: your-access-key
AWS Secret Access Key: your-secret-access-key
Default region name: eu-central-1
Default output format:
```

Example:

![img_55.png](setup-manual-pictures/img_55.png)

### Create a data stream

_With CLI_

Requirements: CLI is installed and the Account is configured.

To create a data stream with 3 shards and the correct name run the following command in the terminal:
```shell script
aws kinesis create-stream --stream-name measurement-kinesis-stream --shard-count 3
```

To make sure everything was successful the describe command after a minute. The response should look like this with a describing JSON response:
```shell script
aws kinesis describe-stream-summary --stream-name measurement-kinesis-stream
```

![img_47.png](setup-manual-pictures/img_47.png)

_With AWS Management Console_

In the management console search for Kinesis Data Streams and select Kinesis.

![img_57.png](setup-manual-pictures/img_57.png)

Select the Kinesis Data Streams radio button and press "Create data stream".

![img_58.png](setup-manual-pictures/img_58.png)

Navigate to Data streams (on the left menu) and press "Create data stream".

![img_59.png](setup-manual-pictures/img_59.png)

Input the name "measurement-kinesis-stream" and select "Provisioned" as the Data stream capacity.

![img_60.png](setup-manual-pictures/img_60.png)

Input 3 provisioned shards.
> **_NOTE:_** While changing the number of shards you will see that the total Write and Read capacity of the data stream change.
> The limits for one shard are 1 MiB/second and 1000 records per second for write operations, and 2 MiB/second for read operations.

![img_61.png](setup-manual-pictures/img_61.png)

Press "Create data stream" and wait some minutes while AWS manages the creation and hosting. A notification will appear after it's done.

You will be redirected to the newly created data stream and see the summary of it with status, additional information and monitoring.

![img_62.png](setup-manual-pictures/img_62.png)

Under data streams the new stream will appear in the list. Here you can click the stream for further information.
![img_63.png](setup-manual-pictures/img_63.png)

> **_NOTE:_** You can also choose another name like my-own-stream for the data stream, but for that replace the stream name in the [Configuration.java](src/main/java/com/hfu/kauz/event_stream/kinesis/Configuration.java) file, so the application can function properly.
```java
static final String streamName = "my-own-stream";
```

### Delete a data stream

_With CLI_

Requirements: CLI is installed and the Account is configured.

To delete a data stream run the following command in the terminal:
```shell script
aws kinesis delete-stream --stream-name measurement-kinesis-stream
```

To make sure everything was successful run the command again after a minute. The response should look like this with the same Exception:

![img_43.png](setup-manual-pictures/img_43.png)

_With AWS Management Console_

Under the data streams check the created stream and select the Delete operation under the Actions dropdown.

![img_44.png](setup-manual-pictures/img_44.png)

A warning popup will appear. Enter delete into the input field and press "Delete".

![img_45.png](setup-manual-pictures/img_45.png)

> **_NOTE:_** Do not forget to delete the stream. This is the most important part, since Kinesis is not free and a running data stream, even if it consumes and sends no data, will generate costs.

### Summing it up
With that the Kinesis Data Stream is successfully created and the application and CLI is configured to the account and new stream. You can now use the application with your own Kinesis and experiment around.

## MongoDB Cluster
To use Change Streams of MongoDB a cluster is needed. Like Kinesis a MongoDB cluster cannot be created and started easily in a local environment. To simplify the setup a Cloud MongoDB Cluster hosted on Mongo Atlas will be created and used for the project.

> **_NOTE:_** There is a free tier for the MongoDB cluster with limited storage. No costs will appear when using this, but be aware of possible storage problems (It can take a long time tho).
> To make sure the cluster has enough space drop the collection once in a while to free up memory.

### Login or create a MongoDB Atlas account
Login in to your MongoDB Atlas account on [MongoDB Atlas Login](https://account.mongodb.com/account/login). 

If you don't have an account create one on [MongoDB Atlas Register](https://account.mongodb.com/account/register).

### Atlas setup
Before creating a cluster some configurations must be set up.

First create an organization via the menu point "Organizations". Give the organization a name and press "Next". You can skip the "Add Members" section and press "Create Organization". After the creation you will be forwarded to the projects view of the organization.

![img_12.png](setup-manual-pictures/img_12.png)

![img_26.png](setup-manual-pictures/img_26.png)

![img_27.png](setup-manual-pictures/img_27.png)

Here you create a project. Give it a name and press "Next". Again skip the "Add Members" part and press "Create Project". You wil be forwarded to the Database menu of the freshly created project.

![img_28.png](setup-manual-pictures/img_28.png)

![img_29.png](setup-manual-pictures/img_29.png)

![img_30.png](setup-manual-pictures/img_30.png)

### Create a MongoDB Cluster
To create a cluster navigate to the Database part of the newly created project. In this site "Build a Database".

![img_31.png](setup-manual-pictures/img_31.png)

Select the Free offering "Shared".

![img_32.png](setup-manual-pictures/img_32.png)

Select a Cloud Provider and a region (e.g. AWS and eu-central-1 region Frankfurt).

![img_35.png](setup-manual-pictures/img_35.png)

Select the M0 Sandbox Cluster.

![img_34.png](setup-manual-pictures/img_34.png)

Give the cluster a name and press "Create Cluster". Atlas will now provision the cluster on the cloud provider.

![img_36.png](setup-manual-pictures/img_36.png)

After everything is done a success message will appear, and you will find the cluster under the database menu.

![img_24.png](setup-manual-pictures/img_24.png)

![img_39.png](setup-manual-pictures/img_39.png)

While Atlas creates the cluster continue with the next section.

### Security and Authentication
To authenticate securely with a user and password Atlas will guide you through a Security Quickstart after pressing "Create Cluster".

Here type in a username (e.g. mongo-user) and password (e.g. Mong0-cluster) for authentication. Press "Create User".

![img_37.png](setup-manual-pictures/img_37.png)

After that you are forwarded to the network connection part of the configuration.

Select "My local environment". Add an entry to the IP access list:
- IP Address: 0.0.0.0/0
- Description: Allow all IP addresses

Even though allowing all IP addresses is not always best practice add the IP address entry to the access list via the "Add Entry" button.

![img_38.png](setup-manual-pictures/img_38.png)

To finish the Quickstart press "Finish and Close".

### Terminate the cluster
To terminate the cluster navigate to the Database part of the project. Click the button with the three dots on the newly created cluster and choose terminate.

![img_10.png](setup-manual-pictures/img_10.png)

A warning popup will appear. Input the cluster name as a confirmation and press the "Terminate" button.

![img_11.png](setup-manual-pictures/img_11.png)

After the shut down the cluster will disappear.

### Configure the Application Properties
To connect to the MongoDB cluster with the application the connection must be established. For this are the application properties responsible.
For a successful connection some properties must be adjusted for the newly create cluster.

On the Database view of the project press the "Connect" button of the created cluster.
![img_40.png](setup-manual-pictures/img_40.png)

Select "Connect your application".

![img_41.png](setup-manual-pictures/img_41.png)

Under the Driver dropdown select Java and as the Version "4.3 or later"

![img_42.png](setup-manual-pictures/img_42.png)

Copy the connection string and modify the password part like in the example below.
```bash
mongodb+srv://mongo-user:<password>@measurement-cluster.endtcgv.mongodb.net/?retryWrites=true&w=majority
# to (with the example authentication data from before)
mongodb+srv://mongo-user:Mong0-cluster@measurement-cluster.endtcgv.mongodb.net/?retryWrites=true&w=majority
```

Replace the connection string in the _quarkus.mongodb.connection-string_ property of the application properties at the end of the file
[application.properties](src/main/resources/application.properties).

```properties
# Application properties for MongoDB Cluster on Mongo Atlas --> AWS
quarkus.mongodb.connection-string = mongodb+srv://mongo-user:Mong0-cluster@measurement-cluster.endtcgv.mongodb.net/?retryWrites=true&w=majority
```

### Summing it up
With that the Mongo Cluster is successfully created and the application is configured to the new cluster. You can now use the application with your own Mongo Cluster and experiment around.

## Recommended Software
Here is a list of useful Software for managing, developing and testing the environment, application and technologies:
- MongoDB Compass
- Conduktor
- Postman
- pgAdmin4
- IntelliJ IDEA
- PyCharm CE
- Visual Studio Code

## The Setup is done
These manual and docker container were tested multiple times on different computers (empty and transient environments). If done correctly you should be able to run the application and do some experiments with the different streaming technologies.

Should you run into problems do not hesitate to contact me. If problems occur often I will add a Troubleshooting section with workarounds until I find a permanent solution.

Have fun hacking and playing around! - Syntax Error