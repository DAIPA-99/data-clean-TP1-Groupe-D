import os
import requests
import numpy as np
import pandas as pd

DATA_PATH = 'data/MMM_MMM_DAE.csv'

def download_data(url, force_download=False, ):
    # Utility function to donwload data if it is not in disk
    data_path = os.path.join('data', os.path.basename(url.split('?')[0]))
    if not os.path.exists(data_path) or force_download:
        # ensure data dir is created
        os.makedirs('data', exist_ok=True)
        # request data from url
        response = requests.get(url, allow_redirects=True)
        # save file
        with open(data_path, "w") as f:
            # Note the content of the response is in binary form: 
            # it needs to be decoded.
            # The response object also contains info about the encoding format
            # which we use as argument for the decoding
            f.write(response.content.decode(response.apparent_encoding))

    return data_path


def load_formatted_data(data_fname:str) -> pd.DataFrame:
    """ One function to read csv into a dataframe with appropriate types/formats.
        Note: read only pertinent columns, ignore the others.
    """
    df = pd.read_csv(
        data_fname,
        usecols= ['nom','lat_coor', 'long_coor','tel1','dermnt','freq_mnt','adr_num','adr_voie','com_cp','com_nom']
        dtype= {'nom':string,'lat_coor':float, 'long_coor':float,'tel1':int,'dermnt':date,'freq_mnt':string,'adr_num':int,'adr_voie':string,'com_cp':int,'com_nom':string
        )
    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to do all sanitizing"""

    """Enleve le + des numeros et enleve les valeurs manquantes"""
    def correction_tel(df):
        caractere_a_supprimer = '+'
        masque = df['tel1'].str.startswith(caractere_a_supprimer)
        df.loc[masque, 'tel1'] = df.loc[masque, 'tel1'].str.replace(caractere_a_supprimer, '')

        import re
        motif_telephone = re.compile(r'^\d{3}\s\d{2}\s\d{2}\s\d{2}\s\d{2}$')
        df_filtre = df[df['tel1'].apply(lambda x: bool(motif_telephone.match(x)))]

        return df_filtre
        
    correction_tel(df)

    """ Les deux fonctions suivantes serviront pour les latitudes longitudes"""
    def check_and_remove_unique_value(df):
        for index, row in df.iterrows():
            if pd.notna(row['lat_coor1']) and pd.isna(row['long_coor1']):
            df.at[index, 'lat_coor1'] = pd.NaN
            elif pd.isna(row['lat_coor1']) and pd.notna(row['long_coor1']):
            df.at[index, 'long_coor1'] = pd.NaN

    
    def remove_non_float_values(df, column_name):
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        df = df.dropna(subset=[column_name])
    
    remove_non_float_values(df, "lat_coor1")
    remove_non_float_values(df, "long_coor1")
    check_and_remove_unique_value(df)

    """La fonction suivante sert à extraire les dates"""
    def filter_date_formats(df, column_name):
        df[column_name] = df[column_name].astype(str)  # Convert column to string to handle mixed types
        date_pattern = re.compile(r'\b\d{2}[/-]\d{2}[/-]\d{4}\b')

        def extract_valid_dates(cell):
            matches = re.findall(date_pattern, cell)
            return ', '.join(matches) if matches else pd.NaT

        df[column_name] = df[column_name].apply(extract_valid_dates)
        df = df.dropna(subset=[column_name])

    filter_date_formats(df, "dermnt")
        
        
    return df


# Define a framing function
def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    df = df.rename(columns={
        'lat_coor': 'Latitude',
        'lon_coor': 'Longitude',
        'tel1': 'Contact',
        'adr_voie': 'Street_Name',
        'adr_num': 'Street_Number',
        'com_cp': 'Postal_Code',
        'com_nom': 'Municipality_Name',
        'nom': 'Defibrillator_Location',
        'freq_mnt':'Maintenance_Frequency',
        'dermnt':'Latest_Maintenance_Date',
    })
    df['Address'] = df['Street_Number'] + df['Street_Name'] + df['Postal_Code'] + df['Municipality_Name']
    return df


# once they are all done, call them in the general clean loading function
def load_clean_data(data_path:str=DATA_PATH)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (load_formatted_data(data_path)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df


# if the module is called, run the main loading function
if __name__ == '__main__':
    load_clean_data(download_data())
