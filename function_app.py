import datetime
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import logging
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="IngestionApiDeltaLake")
@app.schedule(schedule="0 * * 1-5 05-22", arg_name="IngestionApiDeltaLake", run_on_startup=True,
              use_monitor=False) 
def test_function(IngestionApiDeltaLake: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if IngestionApiDeltaLake.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # write some data into a delta table
    df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})
    write_deltalake("./data/delta", df)
    # Load data from the delta table
    dt = DeltaTable("./data/delta")
    df2 = dt.to_pandas()
    assert df == df2
    logging.info('Delta Table Grava com sucesso.', utc_timestamp)