from __future__ import annotations

from press.press.doctype.press_job.press_job import PressJob
from press.workflow_engine.doctype.press_workflow.decorators import flow, task


class AddZFSVolumeJob(PressJob):
	@flow
	def execute(self):
		self.attach_volume()
		self.sync_virtual_machine()
		self.add_volume_to_pool()

	@property
	def volume_size(self):
		return self.arguments_dict.get("size") or 100

	@task
	def attach_volume(self):
		self.virtual_machine_doc.attach_new_volume(
			self.volume_size,
			self.arguments_dict.get("iops"),
			self.arguments_dict.get("throughput"),
		)

	@task
	def sync_virtual_machine(self):
		# Refresh volumes so the newly attached volume is known before extending the pool.
		self.virtual_machine_doc.sync()

	@task
	def add_volume_to_pool(self):
		play = self.server_doc.extend_zfs_pool()
		if not (play and play.status == "Success"):
			raise Exception("Failed to add volume to ZFS pool")
