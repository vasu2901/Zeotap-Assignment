from django.urls import path
from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("connect/", views.connect, name="connect"),
    path("schema/", views.get_schema, name="get_schema"),
    path("preview/", views.preview_data, name="preview_data"),
    path("ingest/", views.ingest_data, name="ingest_data"),
    path("upload/", views.upload_file, name="upload_file"),
    path("preview-flatfile/", views.preview_flatfile, name="preview_flatfile"),
]
