import os
import csv

def import_tpch_data(filepath_in, filepath_out, transforms, header = None):
    """
    imports data into directories tpch_one and tpch_two
    """

    # load data
    with open(filepath_in, 'r') as input_file:
        reader = csv.reader(input_file, delimiter='|')

        # Create an empty list to store the output rows
        output_rows = []
        
        for row in reader:
            # remove final row because it is empty in tpch data
            row.pop()
            # error if transforms is not of same shape
            if len(row) < len(transforms):
                # error
                print("Error: Transform is not of same shape as data")
                return
            for idx, col in enumerate(row):
                if idx < len(transforms):
                    # if none
                    if transforms[idx] == None:
                        continue
                    # if identity
                    elif transforms[idx] == 'delete':
                        row[idx] = 0
                    # round
                    elif transforms[idx] == 'round':
                        row[idx] = round(float(row[idx]))
                    # otherwise apply transform
                    else:
                        row[idx] = round(transforms[idx](float(row[idx])))
                else:
                    # if no transform, set to 0
                    row[idx] = 0

                row[idx] = int(row[idx])

            output_rows.append(row)
            # Open the output CSV file and write the output rows
    with open(filepath_out, 'w', newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerows(output_rows)

    return
    
if __name__ == "__main__":
    datasizes = ['1MB', '10MB', '100MB', '1GB', '10GB']
    for datasize in datasizes:
        # create directories
        os.makedirs(f"tpch_one/{datasize}", exist_ok=True)
        os.makedirs(f"tpch_two/{datasize}", exist_ok=True)

        filepath_in_one = f"../../tpch_workdir/{datasize}/split0.5/orders1.tbl"
        filepath_in_two = f"../../tpch_workdir/{datasize}/split0.5/orders2.tbl"
        
        filepath_out_one = f"tpch_one/{datasize}/orders1.csv"
        filepath_out_two = f"tpch_two/{datasize}/orders2.csv"
                
        # 1|37|O|131251.81|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |
        # 1,37,0,131252,0,0,0,0,0,0
        transforms = [None, None, 'delete', 'round', 'delete', 'delete', 'delete', None, 'delete']
        import_tpch_data(filepath_in_one, filepath_out_one, transforms)
        import_tpch_data(filepath_in_two, filepath_out_two, transforms)