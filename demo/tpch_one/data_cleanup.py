import csv

if __name__ == "__main__":
    
    # file_sizes = ["1MB", "10MB", "100MB", "1GB"]
    file_sizes = ["100MB", "1GB"]
    filename = 'lineitem0.csv'
    filename_out = 'lineitem0_out.csv'

    for filesize in file_sizes:
        
        filepath = "./{}/{}".format(filesize, filename)
        filepath_out = "./{}/{}".format(filesize, filename_out)
        print(filepath)
        # Open the input CSV file
        with open(filepath, 'r') as input_file:
            reader = csv.reader(input_file)

            # Create an empty list to store the output rows
            output_rows = []

            first = True
            # Iterate over each row in the input CSV file
            for row in reader:
                if first:
                    first = False
                    output_rows.append(row)
                    continue

                rounded_value = round(float(row[4]))
                row[4] = str(rounded_value)

                # Round the first column to an integer
                rounded_value = round(float(row[5]))
                row[5] = str(rounded_value)

                # Multiply the second and third columns by 100
                row[6] = str(int(float(row[6]) * 100))
                row[7] = str(int(float(row[7]) * 100))

                # Append the modified row to the output rows list
                output_rows.append(row)

        # Open the output CSV file and write the output rows
        with open(filepath_out, 'w', newline='') as output_file:
            writer = csv.writer(output_file)
            writer.writerows(output_rows)