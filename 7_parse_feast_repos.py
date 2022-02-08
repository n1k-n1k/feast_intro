import os
from pathlib import Path


def get_feast_repos_paths(dir_: str, feast_yaml_name='feature_store.yaml') -> list:
    return [root for root, dirs, files in os.walk(dir_)
            for name in files
            if name.endswith(feast_yaml_name)]


if __name__ == '__main__':
    feast_repos_parent_dir = str(Path(__file__).parent)
    feast_repos_paths = get_feast_repos_paths(feast_repos_parent_dir)
    print(feast_repos_paths)
