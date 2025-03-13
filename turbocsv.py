import os
import mmap
from multiprocessing import Pool, cpu_count

NEW_LINE = b'\n'

BATCH_SIZE = 300 * 1024 * 1024 

def create_json_template(csv_headers):
    """Precreate static parts for each field to avoid per‐line overhead."""
    parts = []
    for i, header in enumerate(csv_headers):
        if i == 0:
            # First field starts with opening brace
            parts.append(b'{"' + header + b'":"')
        else:
            # Other fields start with comma and field name
            parts.append(b',"' + header + b'":"')
        
        # After each field value
        if i < len(csv_headers) - 1:
            # Not the last field, just close quotes
            parts.append(b'"')
        else:
            # Last field, close quotes, object, and add newline
            parts.append(b'"}' + NEW_LINE)
    
    return parts

def process_chunk(args):
    # Instead of preallocating and manually copying into a bytearray,
    # we accumulate completed JSON lines (as bytes) and join at the end.
    chunk_data, csv_headers, template_parts, field_count = args
    out_lines = []
    lines = chunk_data.split(NEW_LINE)
    for line in lines:
        if not line:
            continue
        fields = line.split(b',')
        if len(fields) != field_count:
            continue
        parts = []
        t_idx = 0
        for field in fields:
            parts.append(template_parts[t_idx])
            t_idx += 1
            if field.startswith(b'"') and field.endswith(b'"'):
                parts.append(field)
            else:
                parts.append(field.replace(b'"', b'\\"'))
            parts.append(template_parts[t_idx])
            t_idx += 1
        out_lines.append(b"".join(parts))
    return b"".join(out_lines)

def process_csv(input_file, output_file):
    file_size = os.path.getsize(input_file)
    num_cpus = cpu_count()
    chunk_size = max(file_size // (num_cpus * 8), 8 * 1024 * 1024) 

    with open(input_file, 'rb') as f, open(output_file, 'wb') as out:
        # Memory-map the file for faster I/O.
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        header_end = mm.find(NEW_LINE)
        header_line = mm[:header_end]
        csv_headers = [field.strip(b'"') for field in header_line.split(b',')]
        field_count = len(csv_headers)

        template_parts = create_json_template(csv_headers)

        pool = Pool(num_cpus)

        chunks = []
        pos = header_end + 1
        while pos < file_size:
            chunk_end = min(pos + chunk_size, file_size)
            if chunk_end < file_size:
                mm.seek(chunk_end)
                nl_pos = mm.find(NEW_LINE)
                if nl_pos != -1:
                    chunk_end = nl_pos + 1
            mm.seek(pos)
            chunk_data = mm.read(chunk_end - pos)
            chunks.append((chunk_data, csv_headers, template_parts, field_count))
            pos = chunk_end

        # Process chunks in parallel with imap_unordered cause we don't care with the order.
        for result in pool.imap_unordered(process_chunk, chunks, chunksize=1):
            out.write(result)

        pool.close()
        pool.join()
        mm.close()

if __name__ == "__main__":
    process_csv('./input.csv', './output.jsonl')