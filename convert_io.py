import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from decouple import config


path = r'C:\Users\Kestraa\Documents\personal\projects\backend\scripts\avantvendas\INSS Consignado\Maciça_INSS_2021'
os.chdir(path)

print("Start...\n")

def generate_insert(path):
    
    data = pd.read_csv (path)   
    df = pd.DataFrame(data)


    query = "INSERT INTO avantbd.contrato(cpf, nome, data_nasc, esp, dib, id_banco_pagto, id_agencia_banco, id_orgao_pagador, nu_conta_corrente, id_aps_olconc, cs_meio_pagto, endereco, bairro, municipio, uf, cep, sexo, id_banco_emprestimo, id_contrato_emprestimo, vl_emprestimo, dt_ini_desconto, vl_parcela, tipo_emprestimo, dt_averbacao_consig, situacao_emprestimo, tipo, matricula) VALUES \n"
    print("Create file...\n")
    arquivo = open('script.txt','w')
    arquivo.write(query)
    count = 1

    for row in df.itertuples():
        print("creating line ",count)
        arquivo.write(
            "('"+str(row.cpf)+"', '"
            +str(row.nome)+"', '"
            +str(row.dtnascimento)+"', '"
            +str(row.esp)+"', '"
            +str(row.dib)+"', '"
            +str(row.bancopagto)+"', '"
            +str(row.agenciapagto)+"', '"
            +str(row.orgaopagador)+"', '"
            +str(row.contacorrente)+"', '"
            +str(row.aps)+"', "
            +str(row.meiopagto)+", '"
            +str(row.endereco)+"', '"
            +str(row.bairro)+"', '"
            +str(row.municipio)+"', '"
            +str(row.uf)+"', '"
            +str(row.cep).replace('.0','')+"', '"
            +str(row.sexo)+"', '"
            +str(row.bancoemprestimo)+"', '"
            +str(row.contrato)+"', "
            +str(row.vlemprestimo).replace(',','.')+", '"
            +str(row.iniciododesconto)+"', "
            +str(row.vlparcela).replace(",",".")+", "
            +str(row.tipoemprestimo)+", '"
            +str(row.dataaverbacao)+"', "
            +str(row.situacaoemprestimo)+", 'CONSIG', '"+ str(row.nb) +"'),\n"
        )
        count+=1

    arquivo.close()

def insert_into_db():

    for file in os.listdir():
        if(file.endswith(".csv")):
            print("Reading file "+f"{path}\{file}")
            
            data = pd.read_csv (f"{path}\{file}", encoding = "ISO-8859-1")   
            df = pd.DataFrame(data)

            query = "INSERT INTO avantbd.contrato(cpf, nome, data_nasc, esp, dib, id_banco_pagto, id_agencia_banco, id_orgao_pagador, nu_conta_corrente, id_aps_olconc, cs_meio_pagto, endereco, bairro, municipio, uf, cep, sexo, id_banco_emprestimo, id_contrato_emprestimo, vl_emprestimo, dt_ini_desconto, vl_parcela, tipo_emprestimo, dt_averbacao_consig, situacao_emprestimo, tipo, matricula) VALUES \n"

            try:
                connection = connect()
                count = 1
                for row in df.itertuples():
                    query += "('"+str(row.cpf)+"', '"+str(row.nome)+"', '"+str(row.dtnascimento)+"', '"+str(row.esp)+"', '"+str(row.dib)+"', '"+str(row.bancopagto)+"', '"+str(row.agenciapagto)+"', '"+str(row.orgaopagador)+"', '"+str(row.contacorrente)+"', '"+str(row.aps)+"', "+str(row.meiopagto)+", '"+str(row.endereco)+"', '"+str(row.bairro)+"', '"+str(row.municipio)+"', '"+str(row.uf)+"', '"+str(row.cep).replace('.0','')+"', '"+str(row.sexo)+"', '"+str(row.bancoemprestimo)+"', '"+str(row.contrato)+"', "+str(row.vlemprestimo).replace(',','.')+", '"+str(row.iniciododesconto)+"', "+str(row.vlparcela).replace(",",".")+", "+str(row.tipoemprestimo)+", '"+str(row.dataaverbacao)+"', "+str(row.situacaoemprestimo)+", 'CONSIG', '"+ str(row.nb) +"');"
                    print("creating line "+ str(count) + " \n" + query)
                    try:
                        print("Inserting line "+ str(count))
                        cursor = connection.cursor()
                        cursor.execute(query)
                        connection.commit()
                    except Error as err:
                        print('Error to insert data into db', err)
                        connection.rollback()
                    count+=1
                    query = "INSERT INTO avantbd.contrato(cpf, nome, data_nasc, esp, dib, id_banco_pagto, id_agencia_banco, id_orgao_pagador, nu_conta_corrente, id_aps_olconc, cs_meio_pagto, endereco, bairro, municipio, uf, cep, sexo, id_banco_emprestimo, id_contrato_emprestimo, vl_emprestimo, dt_ini_desconto, vl_parcela, tipo_emprestimo, dt_averbacao_consig, situacao_emprestimo, tipo, matricula) VALUES \n"
                    
            except Error as e:
                print("Error while connecting to MySQL", e)

            finally:
                if connection.is_connected():
                    connection.close()
                    print("MySQL connection is closed")

def generate_update():
    data = pd.read_csv (r'C:\Users\Kestraa\Documents\personal\projects\backend\scripts\avantvendas\INSS Consignado\Maciça_INSS_2021\_AC\_AC.csv', encoding = "ISO-8859-1")   
    df = pd.DataFrame(data)

    print("Create file...\n")
    arquivo = open('script.txt','w')
    count = 1

    for row in df.itertuples():
        print("creating line ",count)
        arquivo.write(
            "UPDATE avantbd.contrato SET matricula = '"+str(row.nb)+"' WHERE cpf = '"+str(row.cpf)+"' AND nome = '"+str(row.nome)+"' AND data_nasc = '"+str(row.dtnascimento)+"' AND id_contrato_emprestimo = '"+str(row.contrato)+"'; \n"
        )
        count+=1

    arquivo.close()

def read_dir():
    for file in os.listdir():
        if(file.endswith(".sql")):
            print(f"{path}\{file}")
            read_text_file(f"{path}\{file}")

def read_text_file(file_path):
    with open(file_path, 'r') as f:
        return (f.read())

def connect():
    connection = mysql.connector.connect(host=config('DATABASE_HOST'), database=config('DATABASE_NAME'), user=config('DATABASE_USR'), password=config('DATABASE_PWD'))
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
    
        return connection

# generate_update()
insert_into_db()