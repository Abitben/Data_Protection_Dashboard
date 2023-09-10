def upload_to_bq(**kwargs):
    from include.sourcing.classes.get_cnil_indexes import GetCnilIndexes
    from include.sourcing.classes.check_if_updated import CheckUpdateCnil
    from include.sourcing.classes.get_cnil_tables_xlsx import UpdateTableCSV
    from include.sourcing.classes.clean_csv_for_bq import CleanCSVForBq
    from include.sourcing.classes.upload_to_bq import UploadToBq

    ti = kwargs['ti']
    
    # Récupérer les valeurs des XComs depuis check_if_updated_task
    task_result = ti.xcom_pull(task_ids='check_if_updated', key='return_value')
    tables, datasets, links = task_result

    for index, table in enumerate(tables):
        dataset = datasets[index]
        link = links[index]
        
        # Utiliser les classes appropriées pour effectuer les étapes nécessaires
        UpdateTableCSV(dataset, table, link)
        CleanCSVForBq(dataset, table)
        UploadToBq(dataset, table)

    print("Chargement vers BigQuery terminé avec succès.")

