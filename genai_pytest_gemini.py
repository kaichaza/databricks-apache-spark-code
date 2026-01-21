%pip install -U google-genai typing_extensions
dbutils.library.restartPython()

import pandas as pd
import os
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType
from google import genai

if "GOOGLE_API_KEY" not in os.environ:
    raise ValueError("CRITICAL ERROR: GOOGLE_API_KEY not found. Please set it in Compute -> Advanced Options -> Environment Variables.")

# This function receives a BATCH of prompts (pandas Series) and returns a BATCH of answers.
# Spark automatically distributes these batches to different workers.
@pandas_udf(StringType())
def ask_gemini_distributed(prompts: pd.Series) -> pd.Series:
    # Initialize the client inside the worker function
    # It will automatically grab 'GOOGLE_API_KEY' from the worker's environment
    client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])
    
    results = []
    for prompt in prompts:
        try:
            # Helper text added to ensure one-word answers
            full_prompt = f"{prompt} Answer in one word."
            
            # Updated syntax for the new google-genai SDK
            response = client.models.generate_content(
                model='gemini-2.0-flash', 
                contents=full_prompt
            )
            results.append(response.text.strip().lower())
        except Exception as e:
            # Return the actual error message for easier debugging
            results.append(f"error: {str(e)}")
            
    return pd.Series(results)

# Create a Spark DataFrame (The "Big Data" part)
data = [
    ("test_01", "What colour is the sky?"), 
    ("test_02", "What is water called when it's frozen?"),
    ("test_03", "What is the capital city of England?")
]
schema = ["test_id", "question"]
df_requests = spark.createDataFrame(data, schema)

# Run the distributed inference
# This line triggers the Spark Job.
df_responses = df_requests.withColumn("gemini_answer", ask_gemini_distributed(col("question")))

# Run the Assertion (Distributed Test)
df_final = df_responses.withColumn(
    "test_passed", 
    # Checking against your specific expected answers
    col("gemini_answer").isin("blue", "ice", "london")
)

# Display results
df_final.display()

# Verify if any test failed
failed_count = df_final.filter(col("test_passed") == False).count()

if failed_count == 0:
    print(" ALL SPARK TESTS PASSED")
else:
    print(f" {failed_count} TESTS FAILED")