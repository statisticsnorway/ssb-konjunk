"""For attechments sent over Altinn3.

This module contains the functions handling attachments and metadata from xml files.
"""

from dataclasses import dataclass
import xml.etree.ElementTree as ET
import re
import datetime
import os
import shutil
import pandas as pd


from .xml_handling import return_txt_xml


def get_fil_ext(filename:str,allowed_extensions: list[str]) -> str|None:
    for ext in allowed_extensions:
        if filename.endswith(ext):
            return ext
    else:
        return None


def make_xlsx(file:str,ext:str) -> str:
    if ext == '.csv':
        df = pd.read_csv(file,sep=';',index_col=False,encoding='latin-1')
        file_out = file.replace(ext,".xlsx")
        df.to_excel(f"{file_out}", index = False)
    elif ext == '.xls':
        df = pd.read_excel(file)
        file_out = file.replace(ext,".xlsx")
        df.to_excel(f"{file_out}", index = False)
        
    return file_out


@dataclass
class folder:
    folder_path: str = None
    attachment: str = None
    xml_file: str = None
    pdf: str = None
    json: str = None
    delregNr: str = None
    periodeFomDato: str = None
    periodeTomDato: str = None
    enhetsNavn: str = None
    enhetsType: str = None
    enhetsOrgNr: str = None
    reporteeOrgNr: str = None
    nickname: str = None
    kontaktPersonNavn: str = None
    kontaktPersonEpost: str = None
    kontaktPersonTelefon: str = None
    kontaktInfoKommentar: str = None



    def get_filenames(self):
        #This function fills inn attachment, xml_file and pdf
        for file in os.listdir(f"{self.folder_path}"):
            if file.endswith(".pdf"):
                self.pdf = f"{self.folder_path}/{file}"
            elif file.endswith(".xml"):
                self.xml_file = f"{self.folder_path}/{file}"
            elif file.endswith(".json"):
                self.json = f"{self.folder_path}/{file}"
            else:
                self.attachment = f"{self.folder_path}/{file}"



    def get_metadata(self,nickname_dict:dict[int,str] = None):
        if self.xml_file is None:
            print(f"Mangler xml fil, sjekke manuelt i mappe {self.folder_path}")
        else:
            # Get a Pandas Dataframe representation of the contents of the file
            tree = ET.parse(self.xml_file)
            root = tree.getroot()

            self.delregNr = return_txt_xml(root,'delregNr')
            self.periodeFomDato = return_txt_xml(root,'periodeFomDato')
            self.periodeTomDato = return_txt_xml(root,'periodeTomDato')
            self.enhetsNavn = return_txt_xml(root,'enhetsNavn')
            self.enhetsType = return_txt_xml(root,'enhetsType')
            self.enhetsOrgNr = return_txt_xml(root,'enhetsOrgNr')
            self.reporteeOrgNr = return_txt_xml(root,'reporteeOrgNr')
            self.kontaktPersonNavn = return_txt_xml(root,'kontaktPersonNavn')
            self.kontaktPersonEpost = return_txt_xml(root,'kontaktPersonEpost')
            self.kontaktPersonTelefon = return_txt_xml(root,'kontaktPersonTelefon')
            self.kontaktInfoKommentar = return_txt_xml(root,'kontaktInfoKommentar')
            
            #doikjedenavn hentes fra en annen plass siden det er lagret lokalt
            if nickname_dict:
                self.nickname = nickname_dict.get(int(self.enhetsOrgNr))


### BRUKER MYE SHUTIL HER, MAA BYTTES UT PAA DAPLA!!!
    def send_attachment(self,path_out:str,manual_files:list[str]) -> int:
        allowed_extensions = ['.xls', '.xlsx', '.csv']
        if self.attachment is None:
            print(f"Finner ikke filen, har du husket å kjøre 'get_filenames()' først?")
            response = 100
        else:
            if not os.path.exists(f"{path_out}/{self.delregNr}"):
                os.mkdir(f"{path_out}/{self.delregNr}")
                os.mkdir(f"{path_out}/{self.delregNr}/manual")
                os.mkdir(f"{path_out}/{self.delregNr}/machine")
            
            if (self.enhetsOrgNr in manual_files) or (self.nickname in manual_files):
                shutil.copy(self.attachment,f"{path_out}/{self.delregNr}/manual/")
                response = 200
            
            ext = get_fil_ext(self.attachment,allowed_extensions)
            
            if ext:
                if ext != '.xlsx':
                    self.attachment = make_xlsx(self.attachment,ext)

                if (os.path.exists(f"{path_out}/{self.delregNr}/machine/{self.nickname}.xlsx")) or (os.path.exists(f"{path_out}/{self.delregNr}/machine/{self.enhetsOrgNr}.xlsx")):
                    shutil.copy(self.attachment,f"{path_out}/{self.delregNr}/manual/")
                    print(f'''
                    Oppdraggiver har levert på nytt.

                    Fil er lagt inn i manuel mappe!!!

                    Gjelder fil: {self.attachment}. 
                    Kontakt person er {self.kontaktPersonNavn}. 
                    Epost:{self.kontaktPersonEpost} Telefon:{self.kontaktPersonTelefon}.
                    Kommentar fra enhet: {self.kontaktInfoKommentar}''')
                    response = 100                        

                else:
                    if self.nickname:
                        shutil.copy(self.attachment,f"{path_out}/{self.delregNr}/machine/{self.nickname}.xlsx")

                    else:
                        shutil.copy(self.attachment,f"{path_out}/{self.delregNr}/machine/{self.enhetsOrgNr}.xlsx")
                    response = 200

            else:
                shutil.copy(self.attachment,f"{path_out}/{self.delregNr}/manual/")
                print(f'''
                Oppdraggiver har levert i feil format.
                
                Fil er lagt inn i manuel mappe!!!
                
                Gjelder fil: {self.attachment}. 
                Kontakt person er {self.kontaktPersonNavn}. 
                Epost:{self.kontaktPersonEpost} Telefon:{self.kontaktPersonTelefon}.
                Kommentar fra enhet: {self.kontaktInfoKommentar}''')
                response = 100

        return response






@dataclass
class attachment():
    path: str
    year: int
    month: int
    list_of_attachments: list[folder] = None
    nickname_dict: dict[int,str] = None
    
    
    def make_objects_list(self) -> list:
        liste_vedlegg = []
        # OS finnes ikke paa dapla maa skrives om!!!
        liste = os.listdir(f"{self.path}/{self.year}/{self.month}")

        for day in liste:
            print("Itererer gjennom dag:",day)
            list_files = os.listdir(f"{self.path}/{self.year}/{self.month}/{day}")
            for file in list_files:
                vedlegg = folder(f"{self.path}/{self.year}/{self.month}/{day}/{file}")
                liste_vedlegg.append(vedlegg)

        self.list_of_attachments = liste_vedlegg

    
    def set_all_data(self):
        for folder in self.list_of_attachments:
            folder.get_filenames()
            folder.get_metadata(self.nickname_dict)

            
    def send_attachments(self,path_loaded:str,path_out:str,manual_files:list[str]):

        i = 0
        for folder in self.list_of_attachments:
            
            if folder not in os.listdir(f"{path_loaded}/{folder.delregNr}/"):
                response = folder.send_attachment(path_out,manual_files)
                shutil.copytree(folder.folder_path,f"{path_loaded}/{folder.delregNr}")
                if response == 200:
                    i = i + 1
            else:
                continue

        print(f"Sendte {i}, vedlegg til mappe {path_out}")
