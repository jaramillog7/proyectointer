#palabaras priorizadas
SST_STRONG_KEYWORDS = [
    "seguridad y salud en el trabajo",
    "sistema de gestion de seguridad y salud en el trabajo",
    "sg-sst",
    "copasst",
    "comite paritario",
    "riesgos laborales",
    "accidente de trabajo",
    "enfermedad laboral",
    "plan de emergencias",
    "trabajo en alturas",
    "espacios confinados",
]
#palabras claves debiles
SST_WEAK_KEYWORDS = [
    "sst",
    "arl",
    "seguridad industrial",
    "salud ocupacional",
]
# Lista de palabras clave que se usan para la búsqueda y el análisis.
# Combinación de las palabras clave fuertes y débiles para ampliar la cobertura del análisis.
# Esta lista es utilizada por el escáner de búsqueda para detectar documentos de SST.
KEYWORDS = SST_STRONG_KEYWORDS + SST_WEAK_KEYWORDS
