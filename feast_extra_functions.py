import pandas as pd
from datetime import datetime

def get_entity_df(no_ids: int, dt: str, join_key: str):
    
    entity_df = pd.DataFrame.from_dict(
        {
            # entity's join key -> entity values
            join_key: range(no_ids),
            
            # "event_timestamp" (reserved key) -> timestamps
            "event_timestamp": [
                                datetime.fromisoformat(dt)
                                for _ in range(no_ids)
                            ],
        
        }
    )
    return entity_df
