def clean_csv(**kwargs):
    from include.sourcing.classes.get_cnil_indexes import GetCnilIndexes
    from include.sourcing.classes.check_if_updated import CheckUpdateCnil
    from include.sourcing.classes.get_cnil_tables_xlsx import UpdateTableCSV
    from include.sourcing.classes.clean_csv_for_bq import CleanCSVForBq
    from include.sourcing.classes.upload_to_bq import UploadToBq
    import os
    
    # Récupérer les valeurs des XComs depuis check_if_updated_task
    print("this is kwargs input:", kwargs)
    try: 
        print(kwargs['xcom_value'][0])
        all_tables = kwargs['xcom_value'][0]
        for index, table_info in enumerate(all_tables):
            print(f'this is table_info', table_info) 
            print(table_info["dataset"])
            print(table_info["table"])
            print(table_info["link"])
            dataset = table_info["dataset"]
            table = table_info["table"]
            link = table_info["link"]

            print('current path', os.getcwd())
            print('path available', os.listdir())
            os.chdir('include')
            os.chdir('sourcing')
            print('changed path to this path', os.getcwd())
            UpdateTableCSV(dataset, table, link)
            CleanCSVForBq(dataset, table)
    except IndexError:
        print("Aucune mise à jour n'a été détectée.")

    print("Cleaning terminé avec succès.")

