from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CreditCardLast4Digits") \
    .getOrCreate()

# Function to mask the credit card number
def mask_credit_card(card_number):
    masked_number = '*' * 12 + card_number[-4:]
    return masked_number

# Get a random or sample 16 digit credit card number from user
credit_card_number = input("Enter a 16 digit credit card number: ")

# Create a Spark DataFrame with a single row containing the credit card number
data = [(credit_card_number,)]
df = spark.createDataFrame(data, ["credit_card_number"])

# Register the UDF with Spark
spark.udf.register("mask_credit_card", mask_credit_card)

# Apply the UDF to mask the credit card number and display the result
df.selectExpr("mask_credit_card(credit_card_number) as masked_credit_card").show(truncate=False)

# Stop the SparkSession
spark.stop()
