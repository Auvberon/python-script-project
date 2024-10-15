from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('connection/details/', views.check_database_connection_details, name='check_database_connection_details'),
    path('register/', views.create_database_user, name='create_user_database'),
    path('unregister/', views.delete_database_user, name='delete_user_database'),
    path('reset-password/', views.update_database_user_password, name='reset user password'),
    path('db/slave/', views.check_database_slave_status, name='check_slave_status'),
]