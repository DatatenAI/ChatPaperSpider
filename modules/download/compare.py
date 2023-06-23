import fitz


CHUNK_SIZE = 256

def compare_files_chunkwise(file1_path, file2_path):
    flag = 0
    with open(file1_path, 'rb') as file1, open(file2_path, 'rb') as file2:
        while True:
            chunk1 = file1.read(CHUNK_SIZE)
            chunk2 = file2.read(CHUNK_SIZE)

            if chunk1 != chunk2:
                print('\n')
                print(chunk1)
                print('\n')
                print(chunk2)
                print(f"chunk {flag} 不一样")
                return False
            flag += 1
            if not chunk1:  # 两个文件都读到了末尾
                return True

# 示例用法
file1_path = '20ef69a4d17ef3aefe69344a910e8fd3-1.pdf'
file2_path = '20ef69a4d17ef3aefe69344a910e8fd3-2.pdf'

if compare_files_chunkwise(file1_path, file2_path):
    print("The files have identical content.")
else:
    print("The files do not have identical content.")
