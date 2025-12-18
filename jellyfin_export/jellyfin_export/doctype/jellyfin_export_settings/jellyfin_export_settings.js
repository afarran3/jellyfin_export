// Copyright (c) 2025, Ahmed Al-farran and contributors
// For license information, please see license.txt

// frappe.ui.form.on("Jellyfin Export Settings", {
// 	refresh(frm) {

// 	},
// });
frappe.ui.form.on("Jellyfin Export Settings", {
  refresh(frm) {
    frm.add_custom_button("Sync All", () => {
      frappe.call("jellyfin_export.api.sync_all").then(() => {
        frappe.msgprint("Queued full sync.");
      });
    });

    frm.add_custom_button("Sync Movies", () => {
      frappe.call("jellyfin_export.api.sync_library", { library_name: "Movies" }).then(() => {
        frappe.msgprint("Queued Movies sync.");
      });
    });

    frm.add_custom_button("Sync Shows", () => {
      frappe.call("jellyfin_export.api.sync_library", { library_name: "Shows" }).then(() => {
        frappe.msgprint("Queued Shows sync.");
      });
    });
  }
});
