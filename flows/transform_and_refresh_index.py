from metaflow import FlowSpec, step, secrets, retry, kubernetes, environment, dbt
import os
from utils.data_tools import DirectorySyncManager


SNOWFLAKE_TABLE = "dim_docs_site_ga"
SNOWFLAKE_SCHEMA = "ob_docs_site_schema"
SECRET_SRCS = ["snowflake-ob-content-universe", "openai_ob_content_universe"]
IMAGE = "public.ecr.aws/outerbounds/ob-content-universe-mf-task-image:latest"
LANCEDB_LOCAL_URI = ".lancedb"
LANCEDB_CONFIG_DIR = os.path.expanduser("~/.lancedb")


class TransformAndIndex(FlowSpec):

    @dbt(models=[SNOWFLAKE_TABLE], project_dir="ob_content_universe_dbt")
    @retry(times=3)
    @secrets(sources=SECRET_SRCS)
    @environment(vars={"LANCEDB_CONFIG_DIR": LANCEDB_CONFIG_DIR})
    @kubernetes(image=IMAGE)
    @step
    def start(self):
        import pandas as pd
        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine
        from llama_index.core.node_parser import HTMLNodeParser
        from llama_index.core import Document, VectorStoreIndex, StorageContext
        from llama_index.vector_stores.lancedb import LanceDBVectorStore

        engine = create_engine(
            URL(
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                account=os.environ["SNOWFLAKE_ACCOUNT_IDENTIFIER"],
                warehouse="google_analytics_wh",
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=SNOWFLAKE_SCHEMA,
                role=os.environ["SNOWFLAKE_OB_CONTENT_UNIVERSE_MF_TASK_ROLE"],
            )
        )

        # fetch result of dbt transformation
        df = pd.read_sql(f"SELECT * FROM {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}", engine)
        df = df[df["content"].notnull()]
        df = df[df["content"] != ""]

        # create documents
        text_list = df.content.values.tolist()
        documents = [
            Document(
                text=row.content,
                metadata={"full_page_url": row.full_page_url, "title": row.title},
                metadata_seperator="::",
                metadata_template="\n{key}=>{value}",
                text_template="Metadata: {metadata_str}\n-----\nContent: {content}",
            )
            for _, row in df.iterrows()
        ]

        # create vector store & index
        parser = HTMLNodeParser()
        nodes = parser.get_nodes_from_documents(documents)
        vector_store = LanceDBVectorStore(uri=LANCEDB_LOCAL_URI)
        storage_context = StorageContext.from_defaults(vector_store=vector_store)
        index = VectorStoreIndex(nodes, storage_context=storage_context)

        # persist index
        index.storage_context.persist(persist_dir=LANCEDB_LOCAL_URI)

        # zip index directory and push to cloud storage
        dir = DirectorySyncManager(root=LANCEDB_LOCAL_URI, run=self)
        dir.upload()

        self.next(self.end)

    @secrets(sources=SECRET_SRCS)
    @environment(vars={"LANCEDB_CONFIG_DIR": LANCEDB_CONFIG_DIR})
    @kubernetes(image=IMAGE)
    @step
    def end(self):
        from llama_index.core import load_index_from_storage, StorageContext
        from llama_index.vector_stores.lancedb import LanceDBVectorStore

        DirectorySyncManager(root=LANCEDB_LOCAL_URI, run=self).download()
        vector_store = LanceDBVectorStore(uri=LANCEDB_LOCAL_URI)
        storage_context = StorageContext.from_defaults(
            vector_store=vector_store, persist_dir=LANCEDB_LOCAL_URI
        )
        index = load_index_from_storage(storage_context)
        query_engine = index.as_query_engine()
        response = query_engine.query("How to manage dependencies in a flow?")
        print(str(response))


if __name__ == "__main__":
    TransformAndIndex()
