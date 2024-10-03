import pandas as pd
#import numpy as np

smp_df = pd.read_csv('data_forbes.csv')

result = smp_df.groupby('company')['profits'].sum(
).reset_index().sort_values(by='profits', ascending=False)

result['rank'] = result['profits'].rank(method='min', ascending=False)

result = result[result['rank'] <= 3][['company', 'profits']]

print(result.head())
