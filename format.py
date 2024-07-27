import json

def write_to_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def process_file(input_filename, output_filename_prefix, batch_size):
    json_objects = []
    cnt = 0
    file_counter = 1
    with open(input_filename, 'r', encoding='utf-8') as f:
        for line in f:
            # cnt += 1
            data = json.loads(line)
            # json_objects.append(data)
            # if cnt % batch_size == 0:
            #     output_filename = f"{output_filename_prefix}_{file_counter}.json"
            #     write_to_file(json_objects, output_filename)
            #     json_objects = []
            #     file_counter += 1
            if len(data['user_id']) > 30:
                print(len(data['user_id'])) # 36

    # Write remaining objects to the last file
    # if json_objects:
    #     output_filename = f"{output_filename_prefix}_{file_counter}.json"
    #     write_to_file(json_objects, output_filename)

# 修改为你的输入文件和输出文件前缀
process_file("Health_and_Personal_Care.jsonl", "Health_and_Personal_Care", 70589)