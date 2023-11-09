# SEGWISE ASSIGNMENT


Steps needed to follow to get the output (Approach 1) -
1. Clone this Repository
2. Download the data from URL - https://drive.google.com/file/d/1N__vlmTKHhaDAbpOtifa4bOCeeDqA4K0/view?usp=sharing
3. Extract the data from the zip file and rename it as **playstore.csv**
4. Place the data file in the root directory of the cloned repo.
5. Hadoop should be configure in your system.
6. Download necessary packages
   1. pip install pyspark
   2. pip install csv

Steps needed to follow to get the output (if spark/hadoop not configured) (Approach 2) -
1. Clone this Repository
2. Install docker on your system
3. Inside the root directory of the cloned repo add zipped data file named as archive.zip
4. Build the image - docker build -t segwise_assignment .
5. docker run -it --rm --name output_gen segwise_assignment bash
6. /opt/spark/bin/spark-submit main.py
7. Using a new terminal - docker cp output_gen:/app/segwise_assignment/output .
8. Now you can exit the 1st terminal where your run the spark-submit command.