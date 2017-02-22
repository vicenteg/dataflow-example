# Pre-requisites

## Get a Google Cloud Platform Project

To try this, you'll need to have set up a Google Cloud Platform project. You can take advantage of the [GCP free trial](https://console.cloud.google.com/freetrial) if you like.

You can skip these steps if you're certain Pub/Sub is already ready to use in your project.

Next, run through the Pub/Sub [Quickstart: Using the Console](https://cloud.google.com/pubsub/docs/quickstart-console) guide to make sure you can use Pub/Sub.

## Set up a compute instance

We will use this instance to work with the code. Use a Google Compute Engine instance to avoid the inevitable "doesn't work for me" scenario. Feel free to adapt this to your laptop if you like.

Navigate to [Compute](https://console.cloud.google.com/compute) and create a n1-standard-1 instance using the **Ubuntu 16.04 LTS** image in the Cloud Platform UI.

Once your instance is created, launch cloud shell. 

Run the following, substituting your instance's name as needed:

```
gcloud compute ssh instance-1
```
 
You will probably be promted to create an ssh key - follow the prompts. A passphrase for your ssh key is not necessary for this workshop.

Then install git, openjdk 8 and maven:

```
sudo apt-get update
sudo apt-get install git openjdk-8-jdk maven
```

Clone this repository:

```
git clone ...
```