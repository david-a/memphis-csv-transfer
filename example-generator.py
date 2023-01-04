def generate_csv_file(filename, num_lines):
    with open(filename, "w") as csv_file:
        csv_file.write("FieldA,FieldB,Notes\r\n")
        line = 1
        while line <= num_lines:
          csv_file.write("{}.a,{}.b,{}.notes\r\n".format(line, line, line))
          line += 1

if __name__ == '__main__':
    generate_csv_file('example-file.csv', 100)

