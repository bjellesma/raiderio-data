import pandas as pd

# Assuming you have the following values from SQL
Q1 = 1500.0  # replace with actual Q1 obtained
Q3 = 3000.0  # replace with actual Q3 obtained

IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Let's assume you have a DataFrame `df` containing the p999 scores
df = pd.DataFrame({
    'p999': [1600, 2900, 3200, 1000, 3500, 2750, 2000, 150, 5000, 2500]
})

# Identify outliers
df['anomaly'] = df['p999'].apply(lambda x: 'Anomaly' if x < lower_bound or x > upper_bound else 'Normal')

print(df)