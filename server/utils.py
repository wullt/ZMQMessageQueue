import os
import datetime
import json

def get_path_filename(message, format=".json"):
    """
    Get the path and filename for a message.
    """
    metadata = message.get("metadata")
    if metadata is None:
        return None
    node_id = metadata.get("node_id")
    if node_id is None:
        return None
    capture_timestamp = metadata.get("capture_timestamp")
    if capture_timestamp is None:
        return None
    try:
        capture_timestamp =datetime.datetime.fromisoformat(capture_timestamp)
    except ValueError:
        return None
    
    date_dir = capture_timestamp.strftime("%Y-%m-%d")
    time_dir = capture_timestamp.strftime("%H")
    path = node_id+"/"+date_dir+"/"+time_dir+"/"
    filename = node_id+"_"+capture_timestamp.strftime("%Y-%m-%dT%H-%M-%SZ") + format

    return path, filename

def save_message(message, base_path):
    """
    Save a message to a file.
    """
    path, filename = get_path_filename(message)
    if path is None or filename is None:
        return False
    if not base_path.endswith("/"):
        base_path += "/"
    path = base_path + path
    if not os.path.exists(path):
        os.makedirs(path)
    with open(path+filename, "w") as f:
        f.write(json.dumps(message))
    return True
    