import requests
import pandas as pd
import numpy as np

SOLR_URL = 'http://localhost:8983/solr/'

def createCollection(p_collection_name):
    response = requests.get(f"{SOLR_URL}admin/collections?action=CREATE&name={p_collection_name}&replicationFactor=1")
    return response.json()

def indexData(p_collection_name, p_exclude_column):
    try:
        df = pd.read_csv('data.csv', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv('data.csv', encoding='latin1')

    df.drop(columns=[p_exclude_column], inplace=True)

    df = df.fillna('')
    df = df.replace([np.inf, -np.inf], '')

    for index, row in df.iterrows():
        doc = row.to_dict()
        for key, value in doc.items():
            if isinstance(value, float):
                doc[key] = str(value)
        requests.post(f"{SOLR_URL}{p_collection_name}/update/json/docs", json=doc)

    requests.post(f"{SOLR_URL}{p_collection_name}/update?commit=true")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    response = requests.get(f"{SOLR_URL}{p_collection_name}/select?q={p_column_name}:{p_column_value}")
    return response.json()

def getEmpCount(p_collection_name):
    response = requests.get(f"{SOLR_URL}{p_collection_name}/select?q=*:*&rows=0")
    return response.json()['response']['numFound']

def delEmpById(p_collection_name, p_employee_id):
    requests.post(f"{SOLR_URL}{p_collection_name}/update/json/docs", json={"id": p_employee_id, "_delete_": True})
    requests.post(f"{SOLR_URL}{p_collection_name}/update?commit=true")

def getDepFacet(p_collection_name):
    response = requests.get(f"{SOLR_URL}{p_collection_name}/select?q=*:*&facet=true&facet.field=Department")
    return response.json()

v_nameCollection = 'Hash_Hemanthraj'
v_phoneCollection = 'Hash_0081'

if __name__ == "__main__":
    print("a & b: Creating collections...")
    print(createCollection(v_nameCollection))
    print(createCollection(v_phoneCollection))

    print("c: Getting employee count for name collection...")
    emp_count_name = getEmpCount(v_nameCollection)
    print(f"Employee count in {v_nameCollection}: {emp_count_name}")

    print("d: Indexing data for name collection, excluding 'Department'...")
    indexData(v_nameCollection, 'Department')

    print("e: Indexing data for phone collection, excluding 'Gender'...")
    indexData(v_phoneCollection, 'Gender')

    print("f: Deleting employee with ID 'E02003' from name collection...")
    delEmpById(v_nameCollection, 'E02003')

    print("g: Getting employee count for name collection after deletion...")
    emp_count_name_after = getEmpCount(v_nameCollection)
    print(f"Employee count in {v_nameCollection} after deletion: {emp_count_name_after}")

    print("h: Searching by Department 'IT' in name collection...")
    search_it = searchByColumn(v_nameCollection, 'Department', 'IT')
    print(f"Search results for Department 'IT': {search_it}")

    print("i: Searching by Gender 'Male' in phone collection...")
    search_male = searchByColumn(v_phoneCollection, 'Gender', 'Male')
    print(f"Search results for Gender 'Male': {search_male}")

    print("j: Searching by Department 'IT' in phone collection...")
    search_it_phone = searchByColumn(v_phoneCollection, 'Department', 'IT')
    print(f"Search results for Department 'IT' in phone collection: {search_it_phone}")

    print("k: Getting department facets for name collection...")
    dept_facet_name = getDepFacet(v_nameCollection)
    print(f"Department facets for {v_nameCollection}: {dept_facet_name}")

    print("l: Getting department facets for phone collection...")
    dept_facet_phone = getDepFacet(v_phoneCollection)
    print(f"Department facets for {v_phoneCollection}: {dept_facet_phone}")
