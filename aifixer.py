#%%
# Loaded variable 'df' from URI: /Users/akuehlka/work/crc/prediction_markets/data/silver/markets.csv
import pandas as pd
import os

def aifixer(df):
    
# %%
    custom_categories = [
        "Crypto",
        "Coronavirus",
        "Politics",
        "Pop-Culture",
        "Sports",
        "Science-Tech-Business",
    ]

    #%%
    from openai import OpenAI
    from dotenv import load_dotenv

    load_dotenv()

    # Ensure API key is set
    if not os.getenv("API_KEY"):
        raise ValueError("OPENAI_API_KEY environment variable must be set")
        
    client = OpenAI(api_key=os.getenv("API_KEY"))

    # %%

    def categorize_text(text: str) -> str:
        """
        Uses GPT-4 to categorize input text into one of the predefined categories.
        
        Args:
            text: String to categorize
            
        Returns:
            String containing the matched category
        """
        # Format categories list for prompt
        categories_str = "\n".join([f"- {c}" for c in custom_categories])
        
        prompt = f"""Given the following categories:

    {categories_str}

    Which single category best matches this text: "{text}"

    Return only the category name, exactly as written above, with no additional text or explanation.

    Do not combite categories."""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=50,
        )
        
        predicted_category = response.choices[0].message.content.strip()
        
        # Validate the response is one of our categories
        if predicted_category not in custom_categories:
            # raise ValueError(f"GPT returned '{predicted_category}' which is not in the predefined categories")
            print(f"GPT returned '{predicted_category}' which is not in the predefined categories")
        return predicted_category

    #%%
    # test the categorize_text function using 5 random rows of the df
    for i, row in df.sample(5).iterrows():
        print(f"Row {i}: {row['question']}")
        print(f"Predicted category: {categorize_text(row['question'])}")
        print()

    # %%
    # add the predicted category to the df and time the process
    import time

    df['predicted_category'] = "" 
    start_time = time.time()

    for i, row in df.iterrows():
        print(f"Row {i}: {row['question']}")
        c = categorize_text(row['question'])
        print(f"Predicted category: {c}")
        df.at[i, 'predicted_category'] = c
        print()

    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")


    # %%
    df.sample(10)[['question', 'category', 'predicted_category']]
    # %%
    # save the df to a csv file
    df.to_csv(r'data/silver/markets_with_ai_categories.csv', index=False)

    return df
    # %%
