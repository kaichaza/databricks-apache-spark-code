# COMMAND ----------
# 1. Install the SDK on all worker nodes (standard pip only installs on driver)
%pip install -q -U google-generativeai

# COMMAND ----------
import google.generativeai as genai
import pandas as pd
import os
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType, BooleanType, StructType, StructField

# --- CONFIGURATION ---
os.environ["GOOGLE_API_KEY"] = "API_KEY_HERE" 

# 2. Define the logic as a Pandas UDF
# This function receives a BATCH of prompts (pandas Series) and returns a BATCH of answers.
# Spark automatically distributes these batches to different workers.
@pandas_udf(StringType())
def ask_gemini_distributed(prompts: pd.Series) -> pd.Series:
    # Initialize inside the function so every worker gets its own client
    genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
    model = genai.GenerativeModel('gemini-2.5-pro')
    
    results = []
    for prompt in prompts:
        try:
            # Helper text added to ensure one-word answers
            full_prompt = f"{prompt} Answer in one word."
            response = model.generate_content(full_prompt)
            results.append(response.text.strip().lower())
        except Exception:
            results.append("error")
            
    return pd.Series(results)

# 3. Create a Spark DataFrame (The "Big Data" part)
# Imagine this list has 1,000 questions, not just 2.
data = [
    ("test_01", "What colour is the sky?"), 
    ("test_02", "What is water called when it's frozen?"),
    ("test_03", "What is the capital city of England?")
]
schema = ["test_id", "question"]
df_requests = spark.createDataFrame(data, schema)

# 4. Run the distributed inference
# This line triggers the Spark Job.
df_responses = df_requests.withColumn("gemini_answer", ask_gemini_distributed(col("question")))

# 5. Run the Assertion (Distributed Test)
df_final = df_responses.withColumn(
    "test_passed", 
    # For this specific example, we just check if it returned a valid word
    col("gemini_answer").isin("blue", "ice", "London")
)

# Display results
df_final.display()

# 6. Verify if ANY test failed
failed_count = df_final.filter(col("test_passed") == False).count()

if failed_count == 0:
    print(" ALL SPARK TESTS PASSED")
else:
    print(f" {failed_count} TESTS FAILED")
