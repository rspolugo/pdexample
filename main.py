import pandas as pd
import sqlalchemy.engine
from pathlib import Path
from utils.tools import list_all_files_in_dir
from typing import List, Dict, Any
from sqlalchemy import create_engine
from utils.config import headers
from utils.secrets import secrets
from datetime import datetime

def read_csv(
    src_folder: Path,
    files: List[str],
    headers: Dict[str, list[str]],
    dataset_name: str,
) -> pd.DataFrame:
    temp_dfs = []
    for file in files:
        print(f"{file=} is being processed...")
        temp_df = pd.read_csv(f"{src_folder}/{file}", names=headers[dataset_name])
        temp_dfs.append(temp_df)

    return pd.concat(temp_dfs)


def create_connection(secrets: Dict[str, Any]) -> sqlalchemy.engine.Engine:
    return create_engine(
        f"postgresql://{secrets['user']}:{secrets['password']}@{secrets['host']}:{secrets['port']}/{secrets['database']}"
    )


if __name__ == "__main__":
    engine = create_connection(secrets)

    profiles_path = Path(__file__).parent / "data" / "profiles"
    profiles_files = list_all_files_in_dir(profiles_path, ".gz")

    profiles_df = read_csv(
        src_folder=profiles_path,
        files=profiles_files,
        headers=headers,
        dataset_name="profiles",
    )

    print("Inserting dataset into database...")
    now = datetime.now()
    profiles_df.to_sql(
        name="profiles", con=engine, if_exists="replace",
    )
    end = datetime.now() - now
    print(f"Data is uploaded, time : {end.seconds} seconds.")

    activity_path = Path(__file__).parent / "data" / "activity"
    activity_files = list_all_files_in_dir(activity_path, ".gz")

    activity_df = read_csv(
        src_folder=activity_path,
        files=activity_files,
        headers=headers,
        dataset_name="activity",
    )

    print("Inserting dataset into database...")
    now = datetime.now()
    activity_df.to_sql(
        name="activity", con=engine, if_exists="replace", chunksize=50000
    )
    end = datetime.now() - now
    print(f"Data is uploaded, time : {end.seconds} seconds.")