# Step 1: Import required libraries
import pandas as pd

# Step 2: Load the dataset
df = pd.read_csv('Tweets.csv')  # make sure the file is in the same folder or give full path

# Step 3: View basic info
print("Shape of dataset:", df.shape)
print("Columns available:", df.columns)
df.head()
