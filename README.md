# Image-Recognition
Here I have created an image recognition pipeline using AWS.
There are 2 EC2 instances, SQS an S3 bucket and AWS Rekognition was used.
Each instance consists of a java application.
Instance 1 will read the images from S3 bucket which was previously created use Rekognition and select the images of cars with 90% above confidence and push them into SQS.
It will be stored in SQS and then instance 2 will pull them and search for any text/numbers in those images after it recognizes them instance 2 will print them out.
...
