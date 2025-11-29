from ninja_extra import api_controller, route
from .schemas import WebhookPayload
from extractor.state import set_status # --- CHANGE: Import set_status function

@api_controller('/webhooks', tags=['Worker Notifications'])
class WebhookController:
    
    @route.post('/schedule/update_status')
    def update_schedule_status(self, payload: WebhookPayload):
        print(f"⚡ [WEBHOOK] Schedule -> {payload.status}")
        # Use the setter function
        set_status('schedule', payload.status)
        return 200

    @route.post('/area/update_status')
    def update_area_status(self, payload: WebhookPayload):
        print(f"⚡ [WEBHOOK] Area -> {payload.status}")
        # Use the setter function
        set_status('area', payload.status)
        return 200