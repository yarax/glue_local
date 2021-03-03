class DynamicFrame:
    @staticmethod
    def fromDF(df, glueContext, ctx):
        print("invoking fromDF..")
    
    # def __init__(self, path, action, target=None):
    #     print("__init__ is not implemented")

    def __init__(self, df):
        print("###########")
        print(df)
        self.df = df
    def toDF(self):
        return self.df

    def _rdd(self):
        print("_rdd is not implemented")

    def with_frame_schema(self, schema):
        print("with_frame_schema is not implemented")

    def schema(self):
        print("schema is not implemented")

    def show(self, num_rows = 20):
        return self.df.show(num_rows)

    def filter(self, f, transformation_ctx = "", info="", stageThreshold=0, totalThreshold=0):
        print("filter is not implemented")

    def wrap_dict_with_dynamic_records(x):
        print("wrap_dict_with_dynamic_records is not implemented")

    def func(iterator):
        print("func is not implemented")

    def mapPartitions(self, f, preservesPartitioning=True, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        print("mapPartitions is not implemented")

    def func(s, iterator):
        print("func is not implemented")

    def map(self, f, preservesPartitioning=False,transformation_ctx = "", info="", stageThreshold=0, totalThreshold=0):
        print("map is not implemented")

    def wrap_dict_with_dynamic_records(x):
        print("wrap_dict_with_dynamic_records is not implemented")

    def func(_, iterator):
        print("func is not implemented")

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False, transformation_ctx = "", info = "", stageThreshold = 0,totalThreshold = 0):
        print("mapPartitionsWithIndex is not implemented")

    def printSchema(self):
        print("printSchema is not implemented")

    def unbox(self, path, format, transformation_ctx="", info = "", stageThreshold = 0, totalThreshold = 0, **options):
        print("unbox is not implemented")

    def drop_fields(self, paths, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("drop_fields is not implemented")

    def select_fields(self, paths, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("select_fields is not implemented")

    def split_fields(self, paths, name1, name2, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("split_fields is not implemented")

    def split_rows(self, comparison_dict, name1, name2, transformation_ctx = "", info= "", stageThreshold = 0, totalThreshold = 0):
        print("split_rows is not implemented")

    def rename_field(self, oldName, newName, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("rename_field is not implemented")

    def write(self, connection_type, connection_options={},               format=None, format_options={}, accumulator_size = 0):
        print("write is not implemented")

    def count(self):
        return self.df.count()

    def spigot(self, path, options={}, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("spigot is not implemented")

    def join(self, paths1, paths2, frame2, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("join is not implemented")

    def unnest(self, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("unnest is not implemented")

    def relationalize(self, root_table_name, staging_path, options={}, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("relationalize is not implemented")

    def applyMapping(self, *args, **kwargs):
        print("applyMapping is not implemented")

    def apply_mapping(self, mappings, case_sensitive = False, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("apply_mapping is not implemented")

    def _to_java_mapping(mapping_tup):
        print("_to_java_mapping is not implemented")

    def resolveChoice(self, specs=None, choice="", database=None, table_name=None,                       transformation_ctx="", info="", stageThreshold=0, totalThreshold=0, catalog_id=None):
        return self.df

    def _to_java_specs(specs_tup):
        print("_to_java_specs is not implemented")

    def mergeDynamicFrame(self, stage_dynamic_frame, primary_keys, transformation_ctx = "", options = {}, info = "", stageThreshold = 0, totalThreshold = 0):
        print("mergeDynamicFrame is not implemented")

    def getNumPartitions(self):
        print("getNumPartitions is not implemented")

    def repartition(self, num_partitions, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("repartition is not implemented")

    def coalesce(self, num_partitions, shuffle = False, transformation_ctx = "", info = "", stageThreshold = 0, totalThreshold = 0):
        print("coalesce is not implemented")

    def errorsAsDynamicFrame(self):
        print("errorsAsDynamicFrame is not implemented")

    def errorsCount(self):
        print("errorsCount is not implemented")

    def stageErrorsCount(self):
        print("stageErrorsCount is not implemented")

    def assertErrorThreshold(self):
        print("assertErrorThreshold is not implemented")

    def __getitem__(self, key):
        print("__getitem__ is not implemented")

    def __len__(self):
        print("__len__ is not implemented")

    def keys(self):
        print("keys is not implemented")

    def values(self):
        print("values is not implemented")

    def select(self, key, transformation_ctx = ""):
        print("select is not implemented")

    def map(self, callable, transformation_ctx = ""):
        print("map is not implemented")

    def flatmap(self, f, transformation_ctx = ""):
        print("flatmap is not implemented")

    def from_rdd(self, data, name, schema=None, sampleRatio=None):
        print("from_rdd is not implemented")

    def from_options(self, connection_type, connection_options={},                      format=None, format_options={}, transformation_ctx="", push_down_predicate = "", **kwargs):
        print("from_options is not implemented")

    def from_catalog(self, database = None, table_name = None, redshift_tmp_dir = "", transformation_ctx = "", push_down_predicate = "", additional_options = {}, catalog_id = None, **kwargs):
        print("from_catalog is not implemented")

    def from_jdbc_conf(self, frame, catalog_connection, connection_options={}, redshift_tmp_dir = "", transformation_ctx=""):
        print("from_jdbc_conf is not implemented")

    def createOrReplaceTempView(self, viewName):
        return self.df.createOrReplaceTempView(viewName)