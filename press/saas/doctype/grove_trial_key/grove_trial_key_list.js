// Copyright (c) 2026, Frappe and contributors
// For license information, please see license.txt

frappe.listview_settings['Grove Trial Key'] = {
	onload(listview) {
		listview.page.add_inner_button(__('Revoke Available Keys'), () =>
			frappe.confirm(
				__('Revoke every available key at Grove? Assigned keys keep running.'),
				() =>
					frappe
						.call({
							method:
								'press.saas.doctype.grove_trial_key.grove_trial_key.revoke_available_keys',
							freeze: true,
						})
						.then(({ message: count }) => {
							frappe.show_alert({
								message: __('{0} keys queued for revoke', [count]),
								indicator: 'green',
							})
							listview.refresh()
						}),
			),
		)
	},
}
