import pandas as pd
from pathlib import Path

# Configuración: nombre del archivo y hoja (puedes cambiarlo o parametrizarlo)
ARCHIVO_CARGA = "Carga_Horaria_2025.xlsx"

# Detecta la hoja más reciente (última) automáticamente
def obtener_ultima_hoja(archivo):
    xl = pd.ExcelFile(archivo)
    return xl.sheet_names[-1] if xl.sheet_names else None

def extraer_horarios_desde_carga_horaria(archivo, sheet):
    df = pd.read_excel(archivo, sheet_name=sheet, skiprows=1)
    df = df[df["CODIGO"].notna()]
    cursos = []
    for _, row in df.iterrows():
        codigo = row.get("CODIGO", "")
        docente = row.get("DOCENTE", "")
        idioma = row.get("IDIOMA", "")
        dias = row.get("DIAS", "")
        horas = row.get("HORAS", "")
        if pd.notna(codigo) and pd.notna(horas):
            cursos.append(f"Curso {codigo} ({idioma}) dictado por {docente}: {dias} de {horas}")
    return cursos

def redactar_instrucciones(cursos):
    instrucciones = "Estos son los horarios de los cursos:\n"
    for curso in cursos:
        instrucciones += f"- {curso}\n"
    instrucciones += ("\nUtiliza esta información para responder dudas sobre horarios, docentes o idiomas de los cursos.")
    return instrucciones

if __name__ == "__main__":
    archivo = ARCHIVO_CARGA
    if not Path(archivo).exists():
        print(f"Archivo {archivo} no encontrado.")
        exit(1)
    sheet = obtener_ultima_hoja(archivo)
    if not sheet:
        print("No se encontró ninguna hoja en el archivo.")
        exit(1)
    cursos = extraer_horarios_desde_carga_horaria(archivo, sheet)
    texto_instrucciones = redactar_instrucciones(cursos)
    # Guarda el resultado en un archivo de texto
    with open("instrucciones_horarios.txt", "w", encoding="utf-8") as f:
        f.write(texto_instrucciones)
    print("Instrucciones generadas en instrucciones_horarios.txt")
