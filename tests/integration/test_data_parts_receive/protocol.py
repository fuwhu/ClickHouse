import struct

from pathlib import Path

def write_string(data: bytearray, s: str):
    encoded = s.encode('utf-8')
    length = len(encoded)
    while length >= 0x80:
        data.append((length & 0x7F) | 0x80)
        length >>= 7
    data.append(length)
    data.extend(encoded)


def write_uint64(data: bytearray, value: int):
    data.extend(struct.pack('<Q', value))


def collect_files(part_path: Path):
    files = []
    for item in sorted(part_path.iterdir()):
        if item.is_file():
            files.append(item)
    return files


def collect_projections(part_path: Path):
    projections = {}
    for item in sorted(part_path.iterdir()):
        if item.is_dir() and item.name.endswith('.proj'):
            proj_files = collect_files(item)
            projections[item.name] = proj_files
    return projections


def calculate_total_size(files, projections):
    total = sum(f.stat().st_size for f in files)
    for proj_files in projections.values():
        total += sum(f.stat().st_size for f in proj_files)
    return total


def build_payload(part_name: str, part_path: Path):
    """
    Build binary payload for DataPartsReceive protocol.
    
    protocol format:
        part name (string)
        total size (uint64)
        projections count (uint64)
        projections
            projection name (string)
            files count (uint64)
            files
                file name (string)
                file size (uint64)
                file content (binary)
        root files count (uint64)
        root files
            root file name (string)
            root file size (uint64)
            root file content (binary)
    """
    data = bytearray()
    
    # part name
    write_string(data, part_name)
    
    root_files = collect_files(part_path)
    projections = collect_projections(part_path)
    
    # total size
    total_size = calculate_total_size(root_files, projections)
    write_uint64(data, total_size)
    
    # projections count
    write_uint64(data, len(projections))
    
    # projections list
    for proj_name, proj_files in projections.items():
        # projection name
        write_string(data, proj_name)
        # files count
        write_uint64(data, len(proj_files))
        # files list
        for file_path in proj_files:
            # file name
            write_string(data, file_path.name)
            # file size
            file_size = file_path.stat().st_size
            write_uint64(data, file_size)
            # file content
            with open(file_path, 'rb') as f:
                data.extend(f.read())
    
    # root files count
    write_uint64(data, len(root_files))
    
    # root files list
    for file_path in root_files:
        # file name
        write_string(data, file_path.name)
        # file size
        file_size = file_path.stat().st_size
        # file size
        write_uint64(data, file_size)
        # file content
        with open(file_path, 'rb') as f:
            data.extend(f.read())
    
    return bytes(data)
