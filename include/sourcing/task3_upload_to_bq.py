def upload_to_bq(**kwargs):
    from include.sourcing.classes.get_cnil_indexes import GetCnilIndexes
    from include.sourcing.classes.check_if_updated import CheckUpdateCnil
    from include.sourcing.classes.get_cnil_tables_xlsx import UpdateTableCSV
    from include.sourcing.classes.clean_csv_for_bq import CleanCSVForBq
    from include.sourcing.classes.upload_to_bq import UploadToBq
    
    # Récupérer les valeurs des XComs depuis check_if_updated_task
    print("this is kwargs input:", kwargs)
    print(kwargs['xcom_value'][0])
    all_tables = kwargs['xcom_value'][0]
    for index, table in enumerate(all_tables):
        print(f'this is table number index {index}', table) 
        print(table["dataset"])
        print(table["table"])
        print(table["link"])
        dataset = table["dataset"]
        table = table["table"]
        link = table["link"]
        # Utiliser les classes appropriées pour effectuer les étapes nécessaires
        UpdateTableCSV(dataset, table, link)
        CleanCSVForBq(dataset, table)
        UploadToBq(dataset, table)

    print("Chargement vers BigQuery terminé avec succès.")

