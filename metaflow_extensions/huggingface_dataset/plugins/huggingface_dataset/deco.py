from metaflow.decorators import StepDecorator
import json
import uuid


CARD_TYPE = "huggingface_dataset"
VERTICAL_HEIGHT_DEFAULT = 550


class HuggingfaceDatasetDecorator(StepDecorator):

    name = CARD_TYPE
    defaults = {
        "id": None,
        "artifact_id": None,
        "vh": 550
    }

    # def step_init(self, flow, graph, step_name, decorators, environment, flow_datastore, logger):

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):

        self.step_name = step_name

        from metaflow import decorators, Parameter

        if self.attributes.get("artifact_id") is not None:
            artifact_handle = self.attributes.get("artifact_id")
            hf_ds_id = getattr(flow, artifact_handle)

            # if isinstance(hf_ds_id, Parameter):
            #     print("Art is a parameter")
            #     for p in flow._get_parameters():
            #         print(p)

            # print(flow, dir(flow), flow._get_parameters())
            # print(f"Artifact handle: {artifact_handle}")
            # print(f"Artifact value: {hf_ds_id}", str(hf_ds_id))
        else:   
            hf_ds_id = self.attributes.get("id")
        card_id = hf_ds_id.replace("/", "_").replace("-", "_")

        for step in flow:
            if step.name == self.step_name:
                break
        
        def _card_deco_already_attached(step, card_id):
            for decorator in step.decorators:
                if decorator.name == "card":
                    if (
                        decorator.attributes["id"]
                        and card_id in decorator.attributes["id"]
                    ):
                        return True
            return False

        if not _card_deco_already_attached(step, card_id):
            decorators._attach_decorators_to_step(
                step, 
                ["card:type=%s,id=%s,options=%s" % (
                    CARD_TYPE, card_id, json.dumps({"id": hf_ds_id, "vh": self.attributes.get("vh", 550)})
                )]
            )

    # def task_pre_step(
    #     self,
    #     step_name,
    #     task_datastore,
    #     metadata,
    #     run_id,
    #     task_id,
    #     flow,
    #     graph,
    #     retry_count,
    #     max_user_code_retries,
    #     ubf_context,
    #     inputs,
    # ):
    #     print("Task pre step")

    # def task_decorate(
    #     self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    # ):
    #     print("Task decorate")
    #     return step_func