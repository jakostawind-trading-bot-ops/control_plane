<script lang="ts">
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';

	let username = $state('');
	let password = $state('');
	let isSubmitting = $state(false);
	let errorMessage = $state('');
	let successMessage = $state('');

	async function login(event: SubmitEvent) {
		event.preventDefault();
		if (isSubmitting) return;

		isSubmitting = true;
		errorMessage = '';
		successMessage = '';

		try {
			const response = await fetch('/api/v1/auth/login', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				credentials: 'include',
				body: JSON.stringify({ username, password }),
				signal: AbortSignal.timeout(15_000)
			});

			if (response.status === 401) {
				errorMessage = 'Неверный username или пароль.';
				return;
			}

			if (!response.ok) {
				errorMessage = 'Не удалось войти. Попробуйте ещё раз.';
				return;
			}

			password = '';
			successMessage = 'Вход выполнен.';
		} catch (error) {
			errorMessage =
				error instanceof DOMException && error.name === 'TimeoutError'
					? 'Сервер не ответил вовремя. Попробуйте ещё раз.'
					: 'Не удалось связаться с сервером. Попробуйте ещё раз.';
		} finally {
			isSubmitting = false;
		}
	}
</script>

<svelte:head>
	<title>Вход — Control Plane</title>
</svelte:head>

<main class="grid min-h-svh place-items-center px-6 py-12">
	<form
		method="POST"
		onsubmit={login}
		aria-label="Вход"
		aria-busy={isSubmitting}
		class="grid w-full max-w-sm gap-5"
	>
		<div class="grid gap-2">
			<label for="username" class="text-sm font-medium">Username</label>
			<Input
				id="username"
				name="username"
				type="text"
				autocomplete="username"
				bind:value={username}
				disabled={isSubmitting}
				required
			/>
		</div>

		<div class="grid gap-2">
			<label for="password" class="text-sm font-medium">Password</label>
			<Input
				id="password"
				name="password"
				type="password"
				autocomplete="current-password"
				bind:value={password}
				disabled={isSubmitting}
				required
			/>
		</div>

		<Button type="submit" disabled={isSubmitting}>
			{isSubmitting ? 'Входим…' : 'Войти'}
		</Button>

		{#if errorMessage}
			<p role="alert" class="text-center text-sm text-destructive">{errorMessage}</p>
		{/if}
		{#if successMessage}
			<p role="status" class="text-center text-sm">{successMessage}</p>
		{/if}
	</form>
</main>
