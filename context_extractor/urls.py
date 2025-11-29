from django.contrib import admin
from django.urls import path
from ninja_extra import NinjaExtraAPI
from extractor import views
from extractor.api import WebhookController

# Initialize Ninja API
api = NinjaExtraAPI(title="Extractor Internal API")
api.register_controllers(WebhookController)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', api.urls),

    # --- HTML Views ---
    path('', views.home_view, name='home'),

    # Schedule Document Routes (These were already correct)
    path('get_status_schedule_doc/', views.route_task_logic, {'task_type': 'schedule'}, name='get_status_schedule_doc'),
    path('schedule_doc_run_status/', views.view_run_status, {'task_type': 'schedule'}, name='schedule_doc_run_status'),
    path('schedule_doc_setup_param/', views.view_setup, {'task_type': 'schedule'}, name='schedule_doc_setup_param'),
    path('schedule_doc_table_data/', views.view_table_data, {'task_type': 'schedule'}, name='schedule_doc_table_data'),

    # Area Definition Routes (RENAMED TO MATCH VIEW LOGIC)
    # 1. Status Check
    path('get_status_area_doc/', views.route_task_logic, {'task_type': 'area'}, name='get_status_area_doc'),
    
    # 2. Run Status
    path('area_doc_run_status/', views.view_run_status, {'task_type': 'area'}, name='area_doc_run_status'),
    
    # 3. Setup (This fixes your specific error)
    path('area_doc_setup_param/', views.view_setup, {'task_type': 'area'}, name='area_doc_setup_param'),
    
    # 4. Table Data
    path('area_doc_table_data/', views.view_table_data, {'task_type': 'area'}, name='area_doc_table_data'),

    path('semantic_search/<str:task_type>/', views.semantic_search_view, name='semantic_search'),
]