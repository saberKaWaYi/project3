from django.urls import path
from .views import *

urlpatterns = [
    path('get_data', get_data, name='get_data'),
    path('menu_data', menu_data, name='menu_data'),
    path('rack_power', rack_power, name='rack_power'),
    path('rack_power_excel', rack_power_excel, name='rack_power_excel'),
    path('rack_power_excel_all', rack_power_excel_all, name='rack_power_excel_all')
]