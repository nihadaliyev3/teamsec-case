from django.urls import path
from .views import SyncTriggerView

urlpatterns = [
    path('sync/', SyncTriggerView.as_view(), name='sync-trigger'),
]
