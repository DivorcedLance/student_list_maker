import pandas as pd
from pathlib import Path
from InquirerPy import inquirer
from datetime import datetime
import re

IDIOMAS_VALIDOS = ["INGLÉS", "PORTUGUÉS", "ITALIANO", "QUECHUA"]
COLUMNAS_FINALES = [
    "CODIGO", "Nivel", "Ciclo", "MODALIDAD", "DOCENTE", "IDIOMA", "DÍAS DETECTADOS",
    "HORARIO DETALLADO", "F. Inicio", "F. Fin", "Parcial", "Final", "Subida de notas",
    "N° Inscritos", "N° Esperado", "N° Aprobados", "N° Desaprobados",
    "N° No asistio (tiene 0)", "Destalle del curso"
]

def clean_df_mes_idioma(excel_path, mes):
    # Lee todos los cursos (de todos los idiomas) del mes seleccionado
    df = pd.read_excel(excel_path, sheet_name=mes, skiprows=1)
    matricula_idx = df[df.iloc[:, 0].astype(str).str.upper().str.contains("MATRÍCULA")].index
    if not matricula_idx.empty:
        df = df.loc[:matricula_idx[0] - 1]

    df["IDIOMA"] = None
    idioma_actual = "INGLÉS"
    for i, row in df.iterrows():
        cod_val = str(row.get("CODIGO", "")).strip().upper()
        ciclo_val = str(row.get("CICLO", "")).strip().upper()
        for idioma in IDIOMAS_VALIDOS[1:]:
            if idioma in cod_val or idioma in ciclo_val:
                idioma_actual = idioma
                break
        df.at[i, "IDIOMA"] = idioma_actual
    df["IDIOMA"] = df["IDIOMA"].ffill()
    df = df[df["CODIGO"].notna()]
    df = df[~df["CODIGO"].astype(str).str.upper().isin(IDIOMAS_VALIDOS)]
    df = df[~df["CODIGO"].astype(str).str.upper().str.contains("CODIGO")]
    df["DOCENTE"] = df["DOCENTE"].ffill()

    # Nivel y Ciclo
    def extraer_nivel_y_ciclo(valor):
        valor = str(valor).strip().upper()
        if "REPASO" in valor:
            return "", "", "repaso"
        match = re.match(r"([BIA])(\d+)", valor)
        if match:
            nivel_map = {"B": "Básico", "I": "Intermedio", "A": "Avanzado"}
            return nivel_map.get(match.group(1), ""), match.group(2), None
        return "", "", None

    niveles = []
    ciclos = []
    overrides = []
    for valor in df.get("CICLO", []):
        try:
            result = extraer_nivel_y_ciclo(valor)
            if not isinstance(result, (list, tuple)) or len(result) != 3:
                result = ("", "", None)
        except Exception:
            result = ("", "", None)
        nivel, ciclo, override = result
        niveles.append(nivel)
        ciclos.append(ciclo)
        overrides.append(override)
    df["Nivel"] = niveles if niveles else None
    df["Ciclo"] = ciclos if ciclos else None
    df["_mod_override"] = overrides if overrides else None

    if "_mod_override" in df.columns and "MODALIDAD" in df.columns:
        df["MODALIDAD"] = df.apply(
            lambda row: row["_mod_override"] if pd.notna(row.get("_mod_override")) else row.get("MODALIDAD"),
            axis=1
        )
        df.drop(columns=["_mod_override"], inplace=True)

    # Días detectados
    def extraer_dias(texto):
        if pd.isna(texto): return []
        texto = texto.upper().replace(" Y ", ", ")
        dias_validos = ["LUNES", "MARTES", "MIÉRCOLES", "JUEVES", "VIERNES", "SÁBADOS", "DOMINGOS"]
        return [d for d in map(str.strip, texto.split(",")) if d in dias_validos]

    df["DÍAS DETECTADOS"] = df["DIAS"].apply(extraer_dias)

    # Inscritos y esperados
    def separar_inscritos(val):
        if pd.isna(val): return pd.Series([None, None])
        val = str(val).strip()
        if "/" in val:
            try:
                num, esperado = val.split("/")
                return pd.Series([int(num), int(esperado)])
            except:
                return pd.Series([None, None])
        elif val.isdigit():
            return pd.Series([int(val), None])
        return pd.Series([None, None])

    if "Nª inscritos" in df.columns:
        df[["N° Inscritos", "N° Esperado"]] = df["Nª inscritos"].apply(separar_inscritos)
    else:
        df["N° Inscritos"] = None
        df["N° Esperado"] = None

    # Horario detallado estructurado
    dia_a_codigo = {
        "LUNES": 0, "MARTES": 1, "MIÉRCOLES": 2,
        "JUEVES": 3, "VIERNES": 4, "SÁBADOS": 5, "DOMINGOS": 6
    }
    def parse_hora(hora_str):
        try:
            return datetime.strptime(hora_str.strip(), "%H:%M").time()
        except:
            return None

    def mapear_horarios_especial(dias, horas):
        if not isinstance(horas, str) or not dias:
            return {}
        bloques = [h.strip() for h in horas.split(",")]
        resultado = {}
        if len(bloques) == 2 and len(dias) >= 3:
            try:
                h1_inicio, h1_fin = map(parse_hora, bloques[0].split(" - "))
                h2_inicio, h2_fin = map(parse_hora, bloques[1].split(" - "))
                resultado[dia_a_codigo[dias[0]]] = (h1_inicio, h1_fin)
                for d in dias[1:]:
                    resultado[dia_a_codigo[d]] = (h2_inicio, h2_fin)
            except:
                return {}
        elif len(bloques) == 1:
            try:
                h_inicio, h_fin = map(parse_hora, bloques[0].split(" - "))
                for d in dias:
                    resultado[dia_a_codigo[d]] = (h_inicio, h_fin)
            except:
                return {}
        return resultado

    df["HORARIO DETALLADO"] = df.apply(lambda row: mapear_horarios_especial(row["DÍAS DETECTADOS"], row["HORAS"]), axis=1)

    # Fechas como date (con formato seguro)
    for col in ["F. Inicio", "F. Fin", "Parcial", "Final", "Subida de notas"]:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors='coerce').dt.date

    # Convertir columnas a enteros o nulo
    columnas_enteras = [
        "Ciclo", "N° Inscritos", "N° Esperado",
        "N° Aprobados", "N° Desaprobados", "N° No asistio (tiene 0)"
    ]
    for col in columnas_enteras:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df[COLUMNAS_FINALES].reset_index(drop=True)


def seleccionar_carga_horaria():
    archivos = [f for f in Path('.').glob("*.xlsx") if not str(f).startswith("~$")]
    if not archivos:
        print("❌ No se encontraron archivos .xlsx en la carpeta actual.")
        exit(1)
    opciones = [{"name": f.name, "value": str(f)} for f in archivos]
    carga_horaria = inquirer.select(
        message="Selecciona el archivo de carga horaria (.xlsx):",
        choices=opciones,
    ).execute()
    return carga_horaria

def seleccionar_mes(carga_horaria):
    xl = pd.ExcelFile(carga_horaria)
    opciones = [{"name": sh, "value": sh} for sh in xl.sheet_names]
    mes = inquirer.select(
        message="Selecciona el mes (sheet):",
        choices=opciones,
    ).execute()
    return mes

def redactar_instrucciones(df):
    instrucciones = "Estos son los horarios de los cursos:\n"
    for _, row in df.iterrows():
        codigo = row.get("CODIGO", "")
    docente = row.get("DOCENTE", "")
    idioma = row.get("IDIOMA", "")
    dias = row.get("DÍAS DETECTADOS", [])
    horas = row.get("HORARIO DETALLADO", {})
    if not codigo or not horas:
        return ""
    dias_str = ', '.join(dias) if dias else "-"
    horas_str = []
    for k, v in horas.items():
        if isinstance(v, tuple) and all(v):
            try:
                dia_nombre = dias[int(k)] if isinstance(k, int) and int(k) < len(dias) else str(k)
            except:
                dia_nombre = str(k)
            horas_str.append(f"{dia_nombre}: {v[0].strftime('%H:%M')} - {v[1].strftime('%H:%M')}")
    horas_str = "; ".join(horas_str) if horas_str else "-"
    instrucciones += f"- Curso {codigo} ({idioma}) dictado por {docente}: {dias_str} {horas_str}\n"
    instrucciones += ("\nUtiliza esta información para responder dudas sobre horarios, docentes o idiomas de los cursos.")
    return instrucciones

if __name__ == "__main__":
    carga_horaria = seleccionar_carga_horaria()
    mes = seleccionar_mes(carga_horaria)
    df = clean_df_mes_idioma(carga_horaria, mes)
    texto = redactar_instrucciones(df)
    print("\n" + texto)
import pandas as pd
from pathlib import Path
from InquirerPy import inquirer
from menu_exportador import clean_df_mes_idioma

def seleccionar_carga_horaria():
    archivos = [f for f in Path('.').glob("*.xlsx") if not str(f).startswith("~$")]
    if not archivos:
        print("❌ No se encontraron archivos .xlsx en la carpeta actual.")
        exit(1)
    opciones = [{"name": f.name, "value": str(f)} for f in archivos]
    carga_horaria = inquirer.select(
        message="Selecciona el archivo de carga horaria (.xlsx):",
        choices=opciones,
    ).execute()
    return carga_horaria

def seleccionar_mes(carga_horaria):
    xl = pd.ExcelFile(carga_horaria)
    opciones = [{"name": sh, "value": sh} for sh in xl.sheet_names]
    mes = inquirer.select(
        message="Selecciona el mes (sheet):",
        choices=opciones,
    ).execute()
    return mes

def redactar_instrucciones(df):
    instrucciones = "Estos son los horarios de los cursos:\n"
    for _, row in df.iterrows():
        codigo = row.get("CODIGO", "")
        docente = row.get("DOCENTE", "")
        idioma = row.get("IDIOMA", "")
        dias = row.get("DÍAS DETECTADOS", [])
        horas = row.get("HORARIO DETALLADO", {})
        if not codigo or not horas:
            continue
        # Formatea días y horas
        dias_str = ', '.join(dias) if dias else "-"
        horas_str = []
        for k, v in horas.items():
            if isinstance(v, tuple) and all(v):
                horas_str.append(f"{dias[k]}: {v[0].strftime('%H:%M')} - {v[1].strftime('%H:%M')}")
        horas_str = "; ".join(horas_str) if horas_str else "-"
        instrucciones += f"- Curso {codigo} ({idioma}) dictado por {docente}: {dias_str} {horas_str}\n"
    instrucciones += ("\nUtiliza esta información para responder dudas sobre horarios, docentes o idiomas de los cursos.")
    return instrucciones

if __name__ == "__main__":
    carga_horaria = seleccionar_carga_horaria()
    mes = seleccionar_mes(carga_horaria)
    df = clean_df_mes_idioma(carga_horaria, mes)
    print(df)
    texto = redactar_instrucciones(df)
    print("\n" + texto)
