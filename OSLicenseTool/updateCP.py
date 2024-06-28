import os

def process_shell_file(file_path, text_to_add, lineOne):
    try:
        with open(file_path, 'r+', encoding='utf-8') as f:
            text_to_add = lineOne+"\n"+text_to_add
            lines = f.readlines()
            if lines and lines[0].strip() == lineOne:
                lines.pop(0)  # 删除第一行 '#!/bin/bash'
                f.seek(0, 0)
                f.write(text_to_add.rstrip('\r\n') + '\n')
                f.writelines(lines)
                print(f"Added copyright notice to {file_path}")
    except Exception as e:
        print(f"Error adding GPL header to {file_path}: {str(e)}")

def add_text_to_file(file_path, text_to_add):
    try:
        with open(file_path, 'r+', encoding='utf-8') as f:
            content = f.read()
            f.seek(0, 0)
            f.write(text_to_add.rstrip('\r\n') + '\n' + content)
    except Exception as e:
        print(f"Error adding text to {file_path}: {str(e)}")

def process_file(file_path, text_to_add):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if text_to_add[:90] not in content:
            add_text_to_file(file_path, text_to_add)
            print(f"Added copyright notice to {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

def process_directory(directory, postfix, content, lineOnes=None):
    for root, _, files in os.walk(directory):
        for file_name in files:
            if file_name.endswith(postfix):
                file_path = os.path.join(root, file_name)
                process_file(file_path, content)
                # for lineOne in lineOnes:
                #     process_shell_file(file_path, content, lineOne)

# the main function
if __name__ == "__main__":

    # 指定要检查的目录
    directory_to_check = '../IGinX'

    # 无须更改
    postfixList = ['.g4', '.thrift', '.java', '.bat', '.py', '.sh']
    cpFileList = ['.copyrightJava', '.copyrightJava','.copyrightJava','.copyrightBat', '.copyrightPySh', '.copyrightPySh']
    # postfix = '.g4'#'.thrift'#'.java'#'.bat'#'.sh'#'.py'#
    # cpFile = ".copyrightJava"#".copyrightBat"#".copyrightPySh"#

    lineOne = ["#!/bin/bash", "#!/bin/sh"]
    lineOneList = [None, None, None, None, None, lineOne]

    for i in range(len(postfixList)):
        postfix = postfixList[i]
        cpFile = cpFileList[i]
        lineOnes = lineOneList[i]
        with open(cpFile, 'r', encoding='utf-8') as f:
            content = f.read()
        process_directory(directory_to_check, postfix, content, lineOnes)
