import pandas as pd
import bokeh
from bokeh.charts import output_file, Chord
from bokeh.io import show, save

import sys

if len(sys.argv) > 1 and sys.argv[1] == "-test":
    df = pd.read_csv("enron.csv")
    source_field = 'source'
    dest_field = 'dest'
    value_field = ''
    output_option = 'output_to_screen'
    output_path = '/tmp/foo.html'
    output_width = 1024
    output_height = 1024
    title_font_size = 32
    title = "Test"
else:
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    sqlCtx = ascontext.getSparkSQLContext()
    df = ascontext.getSparkInputData().toPandas()
    source_field = '%%source_field%%'
    dest_field = '%%dest_field%%'
    value_field = '%%value_field%%'
    output_option = '%%output_option%%'
    output_path = '%%output_path%%'
    output_width = int('%%output_width%%')
    output_height = int('%%output_height%%')
    title_font_size = int('%%title_font_size%%')
    title = '%%title%%'

majVersion = int(bokeh.__version__.split(".")[0])
minVersion = int(bokeh.__version__.split(".")[1])

if minVersion < 12 and majVersion == 0:
    raise Exception("This extension only works with Bokeh v0.12 or higher, you may want to try installing the latest version of the anaconda distribution")

# filter out all records with source=dest
df = df.loc[lambda rec:rec.__getattr__(source_field) != rec.__getattr__(dest_field)]

args = { 'source':source_field, 'target':dest_field }
if value_field:
    args['value'] = value_field


args["width"]=output_width
args["height"]=output_height
args["title"]=title
args["title_text_font_size"]=str(title_font_size)+"pt"

chord_from_df = Chord(df, **args)

if output_option == 'output_to_file':
    if not output_path:
        raise Exception("No output path specified")
else:
    from os import tempnam
    output_path = tempnam()

output_file(output_path, mode="inline")

if output_option == 'output_to_screen':
    show(chord_from_df)
    print("Output should open in a browser window")
else:
    save(chord_from_df)
    print("Output should be saved on the server to path: "+output_path)

