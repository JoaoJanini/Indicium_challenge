import os
def createDir(outdir, outname):
    #Name of the csv containing the table.
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    fullname = os.path.join(outdir, outname)  

    #Transformar dataframe em csv e salvar na pasta apropriada.
    path = fullname
    
    return path