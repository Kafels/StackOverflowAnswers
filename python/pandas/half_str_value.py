# https://stackoverflow.com/questions/67664485/get-first-half-of-string-from-pandas-dataframe-column/67664652#67664652

import pandas as pd

eggs = pd.DataFrame({"id": [0, 1, 2, 3],
                     "text": ["eggs and spam", "green eggs and spam", "eggs and spam2", "green eggs"]})

eggs['truncated_text'] = eggs['text'].apply(lambda text: text[:len(text) // 2])
print(eggs.to_string(index=False))
