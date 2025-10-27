from django.urls import path
from .views import *

urlpatterns = [
    path('get_data', get_data, name='get_data'),
    path('menu_data', menu_data, name='menu_data'),
    path('rack_power', rack_power, name='rack_power'),
    path('rack_power_excel', rack_power_excel, name='rack_power_excel'),
    path('power_csv_all', power_csv_all, name='power_csv_all'),
    path('rack_power_list', rack_power_list, name='rack_power_list'),
    path('rack_power_list_excel', rack_power_list_excel, name='rack_power_list_excel'),
    path('power_csv_all_more', power_csv_all_more, name='power_csv_all_more')
]