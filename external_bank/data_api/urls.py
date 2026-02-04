from django.urls import path
from .views import UploadDataView, UpdateDataView, DataDownloadView

urlpatterns = [
    path('upload/', UploadDataView.as_view(), name='upload'),
    path('update/', UpdateDataView.as_view(), name='update'),
    path('data/', DataDownloadView.as_view(), name='data'),
]