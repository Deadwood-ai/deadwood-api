from datetime import date
from html import escape

from shared.settings import settings


ACCOUNT_URL = 'https://deadtrees.earth/profile'


def processing_failure_holiday_note_is_active(
	until: date | None,
	*,
	today: date | None = None,
) -> bool:
	return until is not None and (today or date.today()) <= until


def dataset_failed_email(
	dataset_id: int,
	file_name: str,
	error_message: str | None = None,
	*,
	today: date | None = None,
) -> tuple[str, str, str]:
	"""Return a user-safe failure email without exposing processor internals."""
	safe_file_name = escape(file_name)
	subject = f'Dataset {dataset_id} - Processing Failed'
	include_holiday_note = processing_failure_holiday_note_is_active(
		settings.PROCESSING_FAILURE_EMAIL_HOLIDAY_NOTE_UNTIL,
		today=today,
	)
	holiday_note_text = (
		'\n\nA note on our current availability\n\n'
		'Most of our team are currently on holiday, so our follow-up may take longer than usual. '
		'We will return to it as soon as the relevant team members are available.'
		if include_holiday_note
		else ''
	)
	holiday_note_html = (
		'<div style="margin: 20px 0; padding: 16px 18px; background: #fff; border: 1px solid #dee2e6; border-radius: 6px;">'
			'<p style="color: #333; margin: 0 0 8px; font-weight: bold;">A note on our current availability</p>'
			'<p style="color: #555; margin: 0;">Most of our team are currently on holiday, so our follow-up may take longer than usual. '
			'We will return to it as soon as the relevant team members are available.</p>'
		'</div>'
		if include_holiday_note
		else ''
	)
	text_body = (
		f'Processing failed for dataset {dataset_id} ({file_name}).\n\n'
		'The DeadTrees team has recorded the failure. Open your dataset status for details and available results.'
		f'\n\nView dataset status: {ACCOUNT_URL}?dataset={dataset_id}'
		'\n\nFor help, contact info@deadtrees.earth.'
		f'{holiday_note_text}\n\n'
		f'Manage processing emails: {ACCOUNT_URL}'
	)
	html_body = f"""
	<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
		<div style="background: #1a1a2e; padding: 20px; border-radius: 8px 8px 0 0;">
			<h1 style="color: #e74c3c; margin: 0; font-size: 20px;">Processing Failed</h1>
		</div>
		<div style="background: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; border-top: none; border-radius: 0 0 8px 8px;">
			<p style="color: #333; margin-top: 0;">Your dataset could not be processed successfully.</p>
			<table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
				<tr>
					<td style="padding: 8px 12px; font-weight: bold; color: #666; width: 120px;">Dataset ID</td>
					<td style="padding: 8px 12px; color: #333;">{dataset_id}</td>
				</tr>
				<tr>
					<td style="padding: 8px 12px; font-weight: bold; color: #666;">File Name</td>
					<td style="padding: 8px 12px; color: #333;">{safe_file_name}</td>
				</tr>
			</table>
			<p style="color: #333;">The DeadTrees team has recorded the failure. Open your dataset status for details and available results.</p>
			<p><a href="{ACCOUNT_URL}?dataset={dataset_id}">View dataset status</a></p>
			{holiday_note_html}
			<p style="color: #666; font-size: 13px;">
				If the problem persists, contact
				<a href="mailto:info@deadtrees.earth" style="color: #2980b9;">info@deadtrees.earth</a>.
			</p>
		</div>
		<p style="color: #999; font-size: 11px; text-align: center; margin-top: 16px;">
			DeadTrees &mdash; <a href="{ACCOUNT_URL}" style="color: #777;">Manage processing emails</a>
		</p>
	</div>
	"""
	return subject, text_body, html_body


def dataset_completed_email(dataset_id: int, file_name: str) -> tuple[str, str, str]:
	"""Return a completion email linking to the canonical dataset route."""
	safe_file_name = escape(file_name)
	dataset_url = f'https://deadtrees.earth/dataset/{dataset_id}'
	subject = f'Dataset {dataset_id} - Processing Complete'
	text_body = (
		f'Processing completed for dataset {dataset_id} ({file_name}).\n\n'
		f'View dataset: {dataset_url}\n\n'
		f'Manage processing emails: {ACCOUNT_URL}'
	)
	html_body = f"""
	<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
		<div style="background: #1a1a2e; padding: 20px; border-radius: 8px 8px 0 0;">
			<h1 style="color: #27ae60; margin: 0; font-size: 20px;">Processing Complete</h1>
		</div>
		<div style="background: #f8f9fa; padding: 20px; border: 1px solid #dee2e6; border-top: none; border-radius: 0 0 8px 8px;">
			<p style="color: #333; margin-top: 0;">Your dataset has been successfully processed and is now available.</p>
			<table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
				<tr>
					<td style="padding: 8px 12px; font-weight: bold; color: #666; width: 120px;">Dataset ID</td>
					<td style="padding: 8px 12px; color: #333;">{dataset_id}</td>
				</tr>
				<tr>
					<td style="padding: 8px 12px; font-weight: bold; color: #666;">File Name</td>
					<td style="padding: 8px 12px; color: #333;">{safe_file_name}</td>
				</tr>
			</table>
			<div style="text-align: center; margin: 24px 0;">
				<a href="{dataset_url}"
				   style="background: #27ae60; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold;">
					View Dataset
				</a>
			</div>
		</div>
		<p style="color: #999; font-size: 11px; text-align: center; margin-top: 16px;">
			DeadTrees &mdash; <a href="{ACCOUNT_URL}" style="color: #777;">Manage processing emails</a>
		</p>
	</div>
	"""
	return subject, text_body, html_body
