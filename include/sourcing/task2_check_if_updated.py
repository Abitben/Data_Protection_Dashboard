
def check_if_updated(**kwargs):
    from include.sourcing.classes.get_cnil_indexes import GetCnilIndexes
    from include.sourcing.classes.check_if_updated import CheckUpdateCnil
    from include.sourcing.classes.get_cnil_tables_xlsx import UpdateTableCSV
    from include.sourcing.classes.clean_csv_for_bq import CleanCSVForBq
    from include.sourcing.classes.upload_to_bq import UploadToBq

    check = CheckUpdateCnil()
    df = check.df_updated
    bool_fr = check.check_new_sanctions_fr()
    bool_eu = check.check_new_decisions_eu()
    datasets = []
    tables = []
    links = []
    if (df['updated'] == True).any():
        print('There are updated datasets')
        for index, row in df.iterrows():
            datasets.append(row['slug'])
            tables.append(row['title'])
            links.append(row['download_url'])
    elif bool_fr:
        print('new sanctions fr found : table add to list')
        dataset_sanctions_fr = check.dataset_sanc_fr
        table_sanctions_fr = check.table_sanc_fr
        datasets.append(dataset_sanctions_fr)
        tables.append(table_sanctions_fr)
        links.append(f'datasets/{dataset_sanctions_fr}/{table_sanctions_fr}')
    elif bool_eu:
        print('new sanctions eu found : table add to list')
        dataset_sanctions_eu = check.dataset_sanc_eu
        table_sanctions_eu = check.table_sanc_eu
        datasets.append(dataset_sanctions_eu)
        tables.append(table_sanctions_eu)
        links.append(f'datasets/{dataset_sanctions_eu}/{table_sanctions_eu}')

    all_datasets = []
    for index, dataset in enumerate(datasets):
        dataset = datasets[index]
        link = links[index]
        table = tables[index]
        table_to_update = {}
        table_to_update = {
            'index': index,
            'dataset': dataset,
            'table': table,
            'link': link
        }
        all_datasets.append(table_to_update)
    
    print(all_datasets)
    return all_datasets